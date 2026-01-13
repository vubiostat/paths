
# Suppress Output when loading libraries

library <- function(...) suppressPackageStartupMessages(base::library(...))

# Data Manipulation
library(tidyverse)
library(lubridate)

# Pull from REDCap
library(redcapAPI) # Read request from REDCap
library(jsonlite)   # Read environment of request

# Pulls from Other Sources
library(sf)    # WARNING sysadmin: spatial processing package, big install
               # Requires libudunits2-dev libgdal-dev libgeos-dev libproj-dev
               # Requires packages units, wk, s2
               # CRAN units has compile issues with later compilers and had to be
               # installed from remotes::install_github("r-quantities/units")

library(httr2)   # httr2 has exponential backoff
library(readxl)  # For Excel reading/writing
library(haven)   # FOR SPSS, Stata and SAS importation

# Email prep
library(emayili) # RDCOMClient is Windows only
library(knitr)   # Format an html email

  ##############################################################################
 #
#
# Parse request and startup
#
# TO RUN LOCALLY, triggers are received to script in JSON
# Sys.setenv(REDCAP_DATA_TRIGGER='{"record":"2","project_id":"223502","instrument":"SDH ETL","instrument_complete":"2"}')
################################################################################

# Helper function
logM <- function(...)
{
  logEvent("INFO", call=.callFromPackage('redcapAPI'), message=paste(...))
  message(...) # Comment this in to debug
}

unlockREDCap(c(rcon  = 'paths_data_builder'),
             keyring = 'API_KEYs',
             envir   = 1,
             url     = 'https://redcap.vumc.org/api/')

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
request$year_vintage <- as.character(request$year_vintage)

if(request$sdh_etl_complete != 'Complete')
{
  logM("Request number ", request$request_number, " is incomplete. Processing skipped.")
  quit(save="no")
}

  ##############################################################################
 #
#
# Download requested data locally and unpack
#
download_tiger_files <- function(year, dir)
{
  logM("Downloading ", year, " TIGER/Line shapefiles.")
  url      <- sprintf(
    "https://www2.census.gov/geo/tiger/TIGER%1$s/COUNTY/%1$s/tl_%1$s_us_county%2$s.zip",
    year, substr(year, 3,4))

  response <-
    request(url)             |>
    req_retry(max_tries = 5) |>
    req_perform(file.path(dir, "counties.zip"))

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
  counties <- st_read(shp_file) |>
    select(GEOID, STATEFP, COUNTYFP, NAME, NAMELSAD) |>
    st_drop_geometry()
  logM("County-level identifiers extracted.")
  counties
}

extract_counties <- function(year, dir)
{
  download_tiger_files(year, dir)
  counties <- process_tiger_files(dir)
  unlink(dir, recursive = FALSE)

  counties
}

extract_chr_data <- function(year, dir)
{
  logM("Extracting ", year, " County Health Rankings National Data.")
  base_url <- "https://www.countyhealthrankings.org/sites/default/files/media/document/analytic_data"
  # This tries multiple possible locations
  url_patterns <- c(paste0(base_url, year, ".csv"),
                    paste0(base_url, year, "_0.csv"),
                    paste0(str_remove(base_url, "media/document/"), year, ".csv"),
                    paste0(str_remove(base_url, "media/document/"), year, "_0.csv"))
  csv_file <- file.path(dir, sprintf("chr_%s.csv", year))
  for (url in url_patterns)
  {
    response <-
      request(url)             |>
      req_retry(max_tries = 5) |>
      req_perform(csv_file)

    if (resp_status(response) == 200)
    {
      logM(year, " County Health Rankings National Data extracted.")

      return(
        read_csv(csv_file, show_col_types = FALSE, skip = 1) |>
          select(-c(statecode, countycode))                  |>
          rename(GEOID = fipscode)                           |>
          mutate(chr_completeness = 1)
      )
    }
  }

  logStop("Unabled to extract the ", year, " County Health Rankings National Data.")
}

extract_ahrf_data <- function(year, dir)
{
  logM("Attempting to extract the ", year, " Area Health Resources Files.")

  # This turns years like "2021_2022" into "2021-2022"
  year <- str_replace(year, "_", "-")

  base_url <- "https://data.hrsa.gov/DataDownload/AHRF/AHRF_"

  # Once again multiple ways it's stored to attempt
  url_patterns <- c(
    paste0(base_url, "SAS_", year, ".zip"),
    paste0(base_url, year, "_SAS.zip")
  )
  zip_file <- file.path(dir, sprintf("ahrf_%s.zip", year))
  for (url in url_patterns)
  {
    response <-
      request(url)             |>
      req_retry(max_tries = 5) |>
      req_perform(zip_file)

    if (resp_status(response) == 200)
    {
      subdir <- paste0(dir, "/ahrf_", year)
      dir.create(subdir, showWarnings = FALSE, recursive = TRUE)
      unzip(zip_file, exdir = subdir)
      sas_file <- list.files(
        subdir,
        pattern = "\\.sas7bdat$",
        full.names = TRUE,
        recursive = TRUE
      )
      ahrf_data <- read_sas(sas_file) |>
        rename(GEOID = f00002)        |>
        mutate(ahrf_completeness = 1)
      logM(year, " Area Health Resources Files extracted.")
      return(ahrf_data)
    }
  }
  logStop(message("Unable to extract the ", year, " Area Health Resources Files."))
}

  ##############################################################################
 #
#
# Transform data into downloadable csv file
#
transform_data <- function(request, dir)
{
  tryCatch({
    if (request$admin_unit == "County or equivalent") {
      counties_data <- extract_counties(request$year_vintage, dir) |>
        mutate(year_vintage = request$year_vintage, year_measure = NA_integer_) |>
        relocate(year_vintage, year_measure, .before = 1)
      compiled_data <- counties_data |> filter(!is.na(year_measure))

      if(request$data_sources___chr == "Checked") {
        years <- request |>
          select(starts_with("year_chr___")) |>
          pivot_longer(cols = everything(),
                       names_to = "year_chr",
                       values_to = "value") |>
          filter(value == "Checked") |>
          pull(year_chr) |>
          str_remove(., "year_chr___")

        for (year in years) {
          chr_data <- extract_chr_data(year, dir)
          if (year %in% compiled_data$year_measure) {
            compiled_data <- compiled_data |>
              left_join(chr_data |> mutate(year_measure = year), by = join_by(year_measure, GEOID))
          } else {
            compiled_data <- compiled_data |>
              bind_rows(counties_data |>
                          mutate(year_measure = as.numeric(year)) |>
                          left_join(chr_data, by = join_by(GEOID)))
          }
        }
      }

      if(request$data_sources___ahrf == "Checked") {
        years <- request |>
          select(starts_with("year_ahrf___")) |>
          pivot_longer(cols = everything(),
                       names_to = "year_ahrf",
                       values_to = "value") |>
          filter(value == "Checked") |>
          pull(year_ahrf) |>
          str_remove(., "year_ahrf___")

        for (year in years) {
          ahrf_data <- extract_ahrf_data(year, dir)
          year_simple <- year |> str_sub(-4)
          if (year_simple %in% compiled_data$year_measure) {
            compiled_data <- compiled_data |>
              left_join(ahrf_data |> mutate(year_measure = year_simple), by = join_by(year_measure, GEOID))
          } else {
            compiled_data <- compiled_data |>
              bind_rows(counties_data |>
                          mutate(year_measure = as.numeric(year_simple)) |>
                          left_join(chr_data, by = join_by(GEOID)))
          }
        }
      }

      file_name <- sprintf("sdh-etl_compiled-data_%s_%s.csv",
                           str_to_lower(request$last_name),
                           Sys.Date())

      file_path <- file.path(dir, file_name)

      write_csv(compiled_data, file_path)

      return(file_path)
    }

    logM("Finished transforming data for request number ", request$request_number)
  }, error = function(e) {
    logStop("Error transforming data for request number ", request$request_number)
  })
}


  ##############################################################################
 #
#
# Create the raw email
#

# FIXME: Include selections
#   <p>Selections:</p>
#<p><ul><li>Administrative unit:</li><li>Vintage year:</li><li>Health indicators:</li></ul></p>
create_email <- function(request, file_path)
{
  body <-
    "# Health Indicators for HIV/AIDS Research

Dear, %s %s,

Your requested dataset has been successfully compiled and is attached to this email.

This email was sent automatically. For any issues or queries, please contact Justin Amarin [<justin.amarin@vumc.org>](mailto:justin.amarin@vumc.org).
"                                             |>
    sprintf(request$title, request$last_name) |>
    knit2html(text = _)

  envelope()                         |>
    from("noreply@vumc.org")         |>
    to(request$email)                |>
    subject("PATHS Dataset Request") |>
    text(body)                       |>
    attachment(
      file_path,
      type="application/zip")        |>
    as.character()                   |>
    cat()
}

  ##############################################################################
 #
#
# Main loop, load the data and create the email
#
dir <- tempfile(pattern="paths_")
if(!dir.create(dir)) logStop("Unable to create directory", dir)
on.exit(unlink(dir, recursive=TRUE))
final <- transform_data(dir, request)
create_email(request, final)

