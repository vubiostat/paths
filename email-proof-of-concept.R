# Suppress Output when loading libraries

library <- function(...) suppressPackageStartupMessages(base::library(...))

# Data Manipulation
library(redcapAPI)
library(tidyverse)
library(lubridate)

# Pull from REDCap
#library(redcapAPI) # Read request from REDCap
library(jsonlite)   # Read environment of request

# Pulls from Other Sources
#library(sf)    # WARNING sysadmin: spatial processing package, big install
               # Requires libudunits2-dev libgdal-dev libgeos-dev libproj-dev
               # Requires packages units, wk, s2
               # CRAN units has compile issues with later compilers and had to be
               # installed from remotes::install_github("r-quantities/units")

#library(httr2)   # httr2 has exponential backoff
#library(readxl)  # For Excel reading/writing
#library(haven)   # FOR SPSS, Stata and SAS importation

# Email prep
library(emayili) # RDCOMClient is Windows only
library(knitr)   # Format an html email

##############################################################################
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

##############################################################################
#
# Create the raw email
#
# FIXME: Include selections
#   <p>Selections:</p>
#<p><ul><li>Administrative unit:</li><li>Vintage year:</li><li>Health indicators:</li></ul></p>
create_email <- function(request)
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
    html(body)
}


##############################################################################
#
# Main loop, load the data and create the email
#
request <- list(title='Mr', last_name='Test', email='cqshelp@vumc.org')
message <- create_email(request)

smtp_server <- Sys.getenv('SMTP_SERVER', '')
if (nchar(smtp_server) > 0) {
  logM("Sending email")
  send_email <- emayili::server(smtp_server)
  request <- tryCatch(
    send_email(message),
    error = function(e) logM(e)
  )
} else {
  logM("SMTP_SERVER is blank")
  cat(as.character(message))
}
