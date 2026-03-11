
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

request <- tryCatch(
  fromJSON(Sys.getenv('REDCAP_DATA_TRIGGER', '')),
  error = function(e) logStop(e)
)

# FIXME: The State of a request needs a proper lifecyle
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
    flags   = "-j", # junk paths (store only filename)
    extras  = "-q"  # quiet nothing to STDOUT
  )

  unlink(tmp_csv)

  filename
}

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

  nm      <- names(result)
  not_key <- nm != 'fips'
  names(result)[not_key] <- paste0(prefix, names(result)[not_key])

  result
}

load_tgr <- function(year) load_data('tgr_', year)[,'fips', drop=FALSE]

loaders <- list(
  svi = function(year) load_data('svi_', year),
  clh = function(year) load_data('clh_', year),
  chr = function(year) load_data('clh_', year)
)

raw_data <- Reduce(
  function(acc, nm)
  {
    parts <-
      regmatches(
        nm,
        regexec("^year_([a-z]+)___([0-9]{4})$", nm)
      )[[1]]

    src  <- parts[2]
    yr   <- parts[3]

    if (is.null(acc[[src]])) acc[[src]] <- list()

    acc[[src]][[yr]] <- loaders[[src]](as.integer(yr))

    acc
  },
  names(request)[
    grepl("^year_[a-z]+___[0-9]{4}$", names(request)) &
      request == "Checked"
  ],
  init = list()
)

# Add year column and rbind within each top-level key
combined <- imap(raw_data, \(years, source_name)
{
  imap(years, \(df, year) mutate(df, year = as.integer(year))) |>
    bind_rows()
})

# Full join all three data.frames by 'fips' and 'YEAR'
result <- reduce(combined, \(a, b) full_join(a, b, by = c("fips", "year")))

requested_data <- function(dir, request)
{
  vintage <- load_tgr(request$year_vintage)
  data    <- left_join(vintage, result, by='fips')

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
