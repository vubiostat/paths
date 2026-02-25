
# Suppress Output when loading libraries

library <- function(...) suppressPackageStartupMessages(base::library(...))

# Data Manipulation
library(tidyverse)
library(lubridate)

# Pull from REDCap
library(redcapAPI) # Read request from REDCap
library(jsonlite)   # Read environment of request

# These should no longer be required
# Pulls from Other Sources
# library(sf)    # WARNING sysadmin: spatial processing package, big install
#                # Requires libudunits2-dev libgdal-dev libgeos-dev libproj-dev
#                # Requires packages units, wk, s2
#                # CRAN units has compile issues with later compilers and had to be
#                # installed from remotes::install_github("r-quantities/units")
# library(readxl)  # For Excel reading/writing
# library(haven)   # FOR SPSS, Stata and SAS importation

library(httr2)   # httr2 has exponential backoff

# Email prep
library(emayili) # RDCOMClient is Windows only
library(knitr)   # Format an html email

DATA_DIR = "./data/"

logM <- function(...)
{
  logEvent("INFO", call=.callFromPackage('redcapAPI'), message=paste(...))
#  message(...)  # Comment this out for production use, i.e. no message output except final email
}

  ##############################################################################
 #
#
# Parse request and startup
#
# TO RUN LOCALLY, triggers are received to script in JSON
# Sys.setenv(REDCAP_DATA_TRIGGER='{"record":"2","project_id":"223502","instrument":"SDH ETL","instrument_complete":"2"}')
################################################################################


unlockREDCap(c(rcon  = 'paths_data_builder'),
             keyring = 'API_KEYs',
             envir   = 1,
             url     = 'https://redcap.vumc.org/api/')

logM("Opened Connection to REDCap pid=", rcon$projectInformation()$project_id)

# logM("JSON Request ", Sys.getenv('REDCAP_DATA_TRIGGER', ''))

# logM("Unpacking request from JSON")
request <- tryCatch(
  fromJSON(Sys.getenv('REDCAP_DATA_TRIGGER', '')),
  error = function(e) logStop(e)
)

# logM("Received parsed JSON ", request)

if(request$instrument_complete != 2)
{
  logM("Request number ", request$request_number, " is incomplete. Processing skipped.")
  quit(save="no")
}

logM("Requesting record", request$record)

request   <- exportRecordsTyped(rcon, records=request$record)
request$year_vintage <- as.character(request$year_vintage)

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
    html(body)                       |>
    attachment(
      file_path,
      type="application/zip")
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
    flags   = "-j"   # junk paths (store only filename)
  )

  unlink(tmp_csv)

  filename
}

requested_data <- function(dir, request)
{
  data <- data.frame(a = 1:3, b=4:6)

  # Savannah Fill this in HERE
  # Use DATA_DIR to find data

  # Lot's of data selecting/mangling

  write_csv_zip(data, dir, file.path(dir, 'tn-paths.csv.zip'))
}

  ##############################################################################
 #
#
# Main loop, load the data and create the email
#
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
  logStop(as.character(message))
}
