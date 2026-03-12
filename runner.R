# runner script for PATHS Project (Rebeiro)
# PATHS => Public Access Tennessee Health Statistics

# Punch list to improve:

# - Need to get a development server strategy as part of work flow.
# - Need to move toward sending links to files instead of emailing full bundle.
# - Files are stored on an open server and downloadable via some hash and deleted after 7 days.
# - Upon a request erroring out, it needs some recovery process. Should we implement a hidden column "processed"?
#  - Error messages for production should go to Justin as well.
# - Data files need to be stripped from github and somehow pulled into project from Sharepoint.

  ###########################################################################
 #
# Statup (load libraries)

# Suppress Output when loading libraries
library <- function(...) suppressPackageStartupMessages(base::library(...))

library(redcapAPI) # Read request from REDCap
library(jsonlite)  # Read environment of request

library(purrr)     # Data transformation
library(dplyr)     # Data transformation

# Email prep
library(emayili)   # RDCOMClient is Windows only
library(knitr)     # Format an html email

DATA_DIR = "./data/"

# TO RUN LOCALLY, triggers are received to script in JSON
# Sys.setenv(REDCAP_DATA_TRIGGER='{"record":"7","project_id":"223502","instrument":"SDH ETL","instrument_complete":"2"}')

  ##############################################################################
 #
# Functions
#

# Create the raw email
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
    html(body)                       |>
    attachment(
      file_path,
      type="application/zip")
}

logM <- function(...)
{
  logEvent("INFO", call=.callFromPackage('redcapAPI'), message=paste(...))
#  message(...)  # Comment this out for production use, i.e. no message output except final email
}

write_csv_zip <- function(data, dir, filename)
{
  tmp_csv <- file.path(dir, 'tn-paths.csv')

  # write data frame
  write.csv(data, tmp_csv, row.names = FALSE)

  # create zip archive
  zip(
    zipfile = filename,
    files   = tmp_csv,
    flags   = "-j", # junk paths (store only filename)
    extras  = "-q"  # quiet nothing to STDOUT
  )

  unlink(tmp_csv)

  filename
}

# This loads a single data file of the corresponding prefix and year
load_data <- function(prefix, year)
{
  result <- tryCatch({
    v_file <- paste0(prefix,year,'.csv')

    read.csv(unz(file.path(DATA_DIR, paste0(v_file, ".zip")), v_file),
             colClasses="character")
  }, error=function(e)
  {
    logStop("Unable to load data for '", prefix, year, ".csv.zip'\n", e)
  })

  # Modified loaded data to include prefix in column name
  nm      <- names(result)
  not_key <- nm != 'fips'
  names(result)[not_key] <- paste0(prefix, names(result)[not_key])

  result
}

load_tgr <- function(year) load_data('tgr_', year)[,'fips', drop=FALSE]

# Example output:
# list(
#   clh = list(2022 = data.frame(...),
#              2023 = data.frame(...)),
#   chr = list(2022 = data.frame(...),
#              2023 = data.frame(...))
# )
load_raw <- function(request)
{
  fields <- names(request)
  # Run an accumulator that fills a list with requested data
  # Starts with an empty list, and for each entry provided
  # it inserts a new entry into the list
  Reduce(
    function(acc, nm)
    {
      # Get source and year of Checked field name
      parts <-
        regmatches(
          nm,
          regexec("^year_([a-z]+)___([0-9]{4})$", nm)
        )[[1]]

      src  <- parts[2]
      yr   <- parts[3]

      # If the source in the list doesn't exist, make it an empty list
      if (is.null(acc[[src]])) acc[[src]] <- list()

      # Now insert the data in the src/year list
      acc[[src]][[yr]] <- load_data(paste0(src,'_'), as.integer(yr))

      # The modified list is returned to continued reduction
      acc
    },
    # Construct list of checked fields that match `year_<src>____<year>`
    fields[
      grepl("^year_[a-z]+___[0-9]{4}$", fields) &
      request == "Checked"
    ],
    init = list() # Initial empty list
  )
}

requested_data <- function(dir, request)
{
  raw_data  <- load_raw(request)

  # Add year column and rbind within each top-level key
  # This modifies every element of the lists to be integer year
  # and it binds the rows within a year
  combined <- imap(
    raw_data,
    \(years, source_name)
    {
      imap(years, \(df, year) mutate(df, year = as.integer(year))) |>
        bind_rows()
    }
  )

  # Load the "skeleton" vintage fips list
  vintage <- load_tgr(request$year_vintage)

  # Full join all source data.frames in list by 'fips' and 'YEAR'
  result  <- reduce(combined, \(a, b) full_join(a, b, by = c("fips", "year")))

  # This drops all rows in the combinded data that do not have a corresponding fips code
  data    <- left_join(vintage, result, by='fips')

  # Write to the directory provided
  write_csv_zip(data, dir, file.path(dir, 'tn-paths.csv.zip'))
}

  ##############################################################################
 #
#
# Main Script
#

unlockREDCap(c(rcon  = 'paths_data_builder'),
             keyring = 'API_KEYs',
             envir   = 1,
             url     = 'https://redcap.vumc.org/api/')

logM("Opened Connection to REDCap pid=", rcon$projectInformation()$project_id)

request <- tryCatch(
  fromJSON(Sys.getenv('REDCAP_DATA_TRIGGER', '')),
  error = function(e) logStop(e)
)

if(request$instrument_complete != 2)
{
  logM("Request number ", request$request_number, " is incomplete. Processing skipped.")
  quit(save="no")
}

logM("Requesting record", request$record)

request              <- exportRecordsTyped(rcon, records=request$record)
request$year_vintage <- as.character(request$year_vintage)

# load the data and create the email

# Need a temp directory to hold the resulting zip file.
dir         <- tempfile(pattern="paths_")
if(!dir.create(dir, showWarnings=FALSE)) logStop("Unable to create directory", dir)
on.exit(unlink(dir, recursive=TRUE))

final       <- requested_data(dir, request)
message     <- create_email(request, final)
smtp_server <- Sys.getenv('SMTP_SERVER', '')

if (nchar(smtp_server) > 0)
{
  logM("Sending email")
  smtp <- emayili::server(smtp_server)
  request <- tryCatch(
    smtp(message),
    error = function(e) logStop(e)
  )
} else
{
  logStop(as.character("NO SMPT SERVER SPECIFIED"))
}
