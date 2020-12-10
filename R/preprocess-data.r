#!/usr/bin/env Rscript


## -----------------------------------------------------------------------------
## Load packages


suppressPackageStartupMessages(library(readr))
suppressPackageStartupMessages(library(GetoptLong))
GetoptLong.options(help_style = "two-column")


## -----------------------------------------------------------------------------
## Data type definition

column_type_definition = cols(
    REPORT_MONTH = col_character(),
    CUSTOMER_CODE = col_character(),
    QUANSAT_1THANG = col_character(),
    QUANSAT_2THANG = col_character(),
    QUANSAT_3THANG = col_character(),
    BI_CUSTOMER_CLASS = col_character(),
    BI_AGE = col_double(),
    BI_YEARS_RELATIONSHIP = col_double(),
    BI_GENDER = col_character(),
    BI_MARITAL_STATUS = col_character(),
    SLSP = col_double(),
    BI_PRODUCT_GROUP = col_character(),
    BI_LOANS_TERM = col_character(),
    GN_GANNHAT = col_double(),
    BI_SLGN_3T = col_character(),
    BI_SLGN_6T = col_character(),
    BI_SLGN_9T = col_character(),
    BI_SLGN_12T = col_character(),
    BI_SODU_3T = col_double(),
    BI_SODU_6T = col_double(),
    BI_SODU_9T = col_double(),
    BI_SODU_12T = col_double(),
    BI_DUNO_3T = col_double(),
    BI_DUNO_6T = col_double(),
    BI_DUNO_9T = col_double(),
    BI_DUNO_12T = col_double(),
    BI_SNQH_MAX_3T = col_double(),
    BI_SNQH_MAX_6T = col_double(),
    BI_SNQH_MAX_9T = col_double(),
    BI_SNQH_MAX_12T = col_double(),
    BI_QH_GANNHAT_3T = col_double(),
    BI_QH_GANNHAT_6T = col_double(),
    BI_QH_GANNHAT_9T = col_double(),
    BI_QH_GANNHAT_12T = col_double(),
    BI_STQH_1_9_3T = col_double(),
    BI_STQH_1_9_6T = col_double(),
    BI_STQH_1_9_9T = col_double(),
    BI_STQH_1_9_12T = col_double(),
    BI_STQH_NHOM1_3T = col_character(),
    BI_STQH_NHOM1_6T = col_character(),
    BI_STQH_NHOM1_9T = col_character(),
    BI_STQH_NHOM1_12T = col_character(),
    STQH_NHOM2TL_3T = col_double(),
    STQH_NHOM2TL_6T = col_double(),
    STQH_NHOM2TL_9T = col_double(),
    STQH_NHOM2TL_12T = col_double(),
    BI_STQH_NHOM2TL_3T = col_character(),
    BI_STQH_NHOM2TL_6T = col_character(),
    BI_STQH_NHOM2TL_9T = col_character(),
    BI_STQH_NHOM2TL_12T = col_character(),
    SHORT_NAME = col_character(),
    COMPANY_BOOK = col_character(),
    COMPANY_NAME_VN = col_character()
)

## -----------------------------------------------------------------------------
## Command line arguments

args = new.env()
args$VERSION = "0.1.0"

GetoptLong(
    "rawdata=s{1}", "Raw file to be pre-processed.",
    "exportpath=s{1}", "Destination folder for interim data.",
    envir = args
)

## -----------------------------------------------------------------------------
## Functions

read_raw_data = function(path, col_types) {

    stopifnot(
        "`path` does not exists" = file.exists(path)
    )

    dt = readr::read_csv(file = path, col_types = col_types)

    stopifnot(
        "number of columns does not match" = all(names(dt) %in% names(col_types$cols))
    )
    
    names(dt) = tolower(names(dt))
    dt
}

clean_raw_data = function(ews_data) {

    dt = ews_data
    target = "quansat_3thang"
    null_columns = c("quansat_2thang", "quansat_1thang", "customer_code",
                     "report_month", "short_name", "company_book", "company_name_vn")

    predictors = setdiff(names(dt), c(target, null_columns))
    numeric_predictors = predictors[sapply(dt[, predictors], class) == "numeric"]
    categ_predictors = predictors[sapply(dt[, predictors], class) == "character"]
    amt_cols = numeric_predictors[grepl("(sodu|duno)", numeric_predictors)]

    dt$bi_marital_status[dt$bi_marital_status %in% "SEPARATED"] = "UNKNOWN"

    ## transform data
    dt[, target] = lapply(dt[, target], factor)
    dt[, categ_predictors] = lapply(dt[, categ_predictors], factor) # for modeling dummy variables
    dt[, amt_cols] = lapply(dt[, amt_cols], function(x) x / 1e6) # for convenience
    dt[, amt_cols] = lapply(dt[, amt_cols], function(x) {x[x == 0] = 0.001; x})

    dt$bi_product_group = forcats::fct_lump(dt$bi_product_group, n = 4)
    dt$bi_loans_term = forcats::fct_lump(dt$bi_loans_term, n = 3)

    dt
}

export_interim_data = function(ews_data, path) {
    saveRDS(ews_data, file = path)
}


## -----------------------------------------------------------------------------
## Main

message("* reading raw EWS data: ", args$rawdata)
ews = read_raw_data(path = args$rawdata, col_types = column_type_definition)

message("* cleaning raw EWS data")
out = clean_raw_data(ews_data = ews)

message("* exporting data to: ", args$exportpath)
export_interim_data(ews_data = out, path = args$exportpath)
