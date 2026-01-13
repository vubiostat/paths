
# Suppress Output when loading libraries

library <- function(...) suppressPackageStartupMessage(library(...))

# Data Manipulation
library(tidyverse)
library(lubridate)

# Pull from REDCap
library(redcapAPI)

# Pulls from Other Sources
library(sf)    # WARNING sysadmin: spatial processing package, big install
               # Requires libudunits2-dev libgdal-dev libgeos-dev libproj-dev
               # Requires packages units, wk, s2
               # CRAN units has compile issues with later compilers and had to be
               # installed from remotes::install_github("r-quantities/units")
library(readxl)
library(httr2) # httr2 has exponential backoff
library(RDCOMClient)
library(haven)
library(jsonlite)

# FIXME: This library shouldn't be used in production
library(here)

logM <- function(...)
{
  logEvent("INFO", call=.callFromPackage('redcapAPI'), message=paste(...))
  message(...) # Comment this in to debug
}

unlockREDCap(c(rcon  = 'paths_data_builder'),
             keyring = 'API_KEYs',
             envir   = 1,
             url     = 'https://redcap.vumc.org/api/')

# TO RUN LOCALLY, triggers are received to script in JSON
# Sys.setenv(REDCAP_DATA_TRIGGER='{"record":"2","project_id":"123456","instrument":"demographics","instrument_complete":"2"}')

get_record_id <- function()
{
  logM("Unpacking request from JSON")
  request <- tryCatch(
    fromJSON(Sys.getenv('REDCAP_DATA_TRIGGER', '')),
    error = function(e) logStop(e)
  )
  request$record
}

request   <- exportRecordsTyped(rcon, records=get_record_id())


download_tiger_files <- function(year, dir)
{
  logM("Downloading ", year, " TIGER/Line shapefiles.")
  url      <- sprintf(
    "https://www2.census.gov/geo/tiger/TIGER%1$s/COUNTY/tl_%1$s_us_county.zip",
    year)

  response <-
    request(url) |>
    req_retry(max_tries = 5) |>
    req_perform(file.path(dir, "counties.zip"))

  response <- GET(url, write_disk(zip_file, overwrite = TRUE))
  if (resp_status(response) == 200)
  {
    logM(year, " TIGER/Line shapefiles downloaded.")
    unzip(zip_file, exdir = dir)
  } else
  {
    logStop(year, " TIGER/Line failed to download.")
  }
}

process_tiger_files <- function(dir)
{
  logM("Attempting to extract county-level identifiers...")
  shp_file <- list.files(dir, pattern = "\\.shp$", full.names = TRUE)
  counties <- st_read(shp_file) %>%
    select(GEOID, STATEFP, COUNTYFP, NAME, NAMELSAD) %>%
    st_drop_geometry()
  logM("County-level identifiers extracted.")
  counties
}

extract_counties <- function(year)
{
  download_tiger_files(year, dir)
  counties <- process_tiger_files(dir)
  unlink(dir, recursive = FALSE)
  return(counties)
}

extract_chr_data <- function(year, dir)
{
  logM("Extracting ", year, " County Health Rankings National Data...")
  base_url <- "https://www.countyhealthrankings.org/sites/default/files/media/document/analytic_data"
  url_patterns <- c(paste0(base_url, year, ".csv"),
                    paste0(base_url, year, "_0.csv"),
                    paste0(str_remove(base_url, "media/document/"), year, ".csv"),
                    paste0(str_remove(base_url, "media/document/"), year, "_0.csv"))
  csv_file <- file.path(dir, sprintf("chr_%s.csv", year))
  for (url in url_patterns) {
    response <- GET(url, write_disk(csv_file, overwrite = TRUE))
    if (status_code(response) == 200) {
      message(year, " County Health Rankings National Data extracted.")
      chr_data <- read_csv(csv_file, show_col_types = F, skip = 1) %>%
        select(-c(statecode, countycode)) %>%
        rename(GEOID = fipscode) %>%
        mutate(chr_completeness = 1)
      return(chr_data)
      }
    }
  stop(message("Unable to extract the ", year, " County Health Rankings National Data..."))
}

extract_ahrf_data <- function(year, dir) {
  year <- str_replace(year, "_", "-")
  message("Attempting to extract the ", year, " Area Health Resources Files...")
  base_url <- "https://data.hrsa.gov/DataDownload/AHRF/AHRF_"
  url_patterns <- c(
    paste0(base_url, "SAS_", year, ".zip"),
    paste0(base_url, year, "_SAS.zip")
  )
  zip_file <- file.path(dir, sprintf("ahrf_%s.zip", year))
  for (url in url_patterns) {
    response <- GET(url, write_disk(zip_file, overwrite = TRUE))
    if (status_code(response) == 200) {
      subdir <- paste0(dir, "/ahrf_", year)
      dir.create(subdir, showWarnings = FALSE, recursive = TRUE)
      unzip(zip_file, exdir = subdir)
      sas_file <- list.files(
        subdir,
        pattern = "\\.sas7bdat$",
        full.names = TRUE,
        recursive = TRUE
      )
      ahrf_data <- read_sas(sas_file) %>%
        rename(GEOID = f00002) %>%
        mutate(ahrf_completeness = 1)
      message(year, " Area Health Resources Files extracted.")
      return(ahrf_data)
    }
  }
  stop(message("Unable to extract the ", year, " Area Health Resources Files..."))
}

parse_trigger <- function(data_string) {
  message("Parsing the HTTP POST request...")
  url_format <- paste0("http://placeholder.domain?", data_string)
  parsed_trigger <- httr::parse_url(url_format)$query
  return(parsed_trigger)
}

fetch_inputs <- function(redcap_url, project_id, record) {
  message("Fetching user inputs from REDCap...")
  api_token <- Sys.getenv(paste0("api_key_", project_id))
  inputs <- redcap_read(redcap_uri = redcap_url,
                        token = api_token,
                        records = record,
                        raw_or_label = "label")$data
  return(inputs)
}

transform_data <- function(parsed_trigger, dir) {
  if (parsed_trigger$sdh_etl_complete == "2") {
    inputs <- fetch_inputs(redcap_url = paste0(parsed_trigger$redcap_url, "api/"),
                           project_id = parsed_trigger$project_id,
                           record = parsed_trigger$record)
    tryCatch({
      if (inputs$admin_unit == "County or equivalent") {
        counties_data <- extract_counties(inputs$year_vintage) %>%
          mutate(year_vintage = inputs$year_vintage, year_measure = NA_integer_) %>%
          relocate(year_vintage, year_measure, .before = 1)
        compiled_data <- counties_data %>% filter(!is.na(year_measure))

        if(inputs$data_sources___chr == "Checked") {
          years <- inputs %>%
            select(starts_with("year_chr___")) %>%
            pivot_longer(cols = everything(),
                         names_to = "year_chr",
                         values_to = "value") %>%
            filter(value == "Checked") %>%
            pull(year_chr) %>%
            str_remove(., "year_chr___")

          for (year in years) {
            chr_data <- extract_chr_data(year, dir)
            if (year %in% compiled_data$year_measure) {
              compiled_data <- compiled_data %>%
                left_join(chr_data %>% mutate(year_measure = year), by = join_by(year_measure, GEOID))
            } else {
              compiled_data <- compiled_data %>%
                bind_rows(counties_data %>%
                            mutate(year_measure = as.numeric(year)) %>%
                            left_join(chr_data, by = join_by(GEOID)))
            }
          }
        }

        if(inputs$data_sources___ahrf == "Checked") {
          years <- inputs %>%
            select(starts_with("year_ahrf___")) %>%
            pivot_longer(cols = everything(),
                         names_to = "year_ahrf",
                         values_to = "value") %>%
            filter(value == "Checked") %>%
            pull(year_ahrf) %>%
            str_remove(., "year_ahrf___")

          for (year in years) {
            ahrf_data <- extract_ahrf_data(year, dir)
            year_simple <- year %>% str_sub(-4)
            if (year_simple %in% compiled_data$year_measure) {
              compiled_data <- compiled_data %>%
                left_join(ahrf_data %>% mutate(year_measure = year_simple), by = join_by(year_measure, GEOID))
            } else {
              compiled_data <- compiled_data %>%
                bind_rows(counties_data %>%
                            mutate(year_measure = as.numeric(year_simple)) %>%
                            left_join(chr_data, by = join_by(GEOID)))
            }
          }
        }

        file_name <- sprintf("sdh-etl_compiled-data_%s_%s.csv",
                             str_to_lower(inputs$last_name),
                             Sys.Date())

        file_path <- file.path(dir, file_name)

        write_csv(compiled_data, file_path)

        return(
          tibble(
            title = inputs$title,
            last_name = inputs$last_name,
            email_address = inputs$email_address,
            file_path = file_path
          )
        )
      }

      message("Finished transforming data for request number ", parsed_trigger$record)
    }, error = function(e) {
      message("Error transforming data for request number ", parsed_trigger$record)
    })
  } else {
    message("Request number ", parsed_trigger$record, " is incomplete. Processing skipped.")
  }
}

load_data <- function(title, last_name, email_address, file_path) {
  outlook <- COMCreate("Outlook.Application.16")
  email <- outlook$CreateItem(0)
  email$Attachments()$Add(file_path)
  email[["To"]] <- email_address
  email[["Subject"]] <- paste0("The dataset you requested is ready!")
  email[["HTMLBody"]] <- paste0("<html><head><style>
  body {font-family: Verdana, sans-serif; font-size: 10pt;}
  .name {margin: 0px; padding-bottom: 3pt;}
  .affiliation {font-size: 9pt; margin: 0px; padding: 0px;}
  .disclaimer {font-size: 9pt; margin: 0px; padding: 0px;}
  hr {height: 1px; background-color: #A9A9A9; border:none;}
  </style></head>
  <body>
  <p><b>Health Indicators for HIV/AIDS Research</b></p>
  <p>Dear ", paste(title, last_name), ",</p>
  <p>Your requested dataset has been successfully compiled and is attached to this email.</p>
  <p>Selections:</p>
  <p><ul><li>Administrative unit:</li><li>Vintage year:</li><li>Health indicators:</li></ul></p>
  <hr>
  <p class='disclaimer'><span style='color: rgb(127, 127, 127);'>🤖 This email was sent automatically. For any issues or queries, please contact Justin Amarin at </span><a href='mailto:justin.amarin@vumc.org' target='_blank' style='color: rgb(127, 127, 127);'><u>justin.amarin@vumc.org</u></a><span style='color: rgb(127, 127, 127);'>.</span></p>
  </body>
  </html>")

  email$Send()
}

req <- tibble(REQUEST_METHOD = character(), postBody = character()) %>% add_row(REQUEST_METHOD = "POST", postBody = "redcap_url=https%3A%2F%2Fredcap.vumc.org%2F&project_url=https%3A%2F%2Fredcap.vumc.org%2Fredcap_v14.6.4%2Findex.php%3Fpid%3D197463&project_id=197463&username=%5Bsurvey+respondent%5D&record=1&instrument=sdh_etl&sdh_etl_complete=2")

#* @post /redcap-trigger
#* @get /redcap-trigger

function(req) {
  if (req$REQUEST_METHOD == "GET") {
    return(message = "The URL is valid.")
  } else if (req$REQUEST_METHOD == "POST") {
    tryCatch({
      message("Received the HTTP POST request.")
      parsed_trigger <- parse_trigger(req$postBody)
      dir <- file.path(here(), sprintf("request-number-%s", parsed_trigger$record))
      dir.create(dir, showWarnings = FALSE, recursive = TRUE)
      outputs <- transform_data(parsed_trigger, dir)
      message("Finished processing data for request number ", parsed_trigger$record)
      load_data(outputs$title, outputs$last_name, outputs$email_address, outputs$file_path)
    }, error = function(e) {
      message("Error processing data for request number ", parsed_trigger$record)
    })
  }
}
