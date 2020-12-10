#!/usr/bin/env Rscript


## -----------------------------------------------------------------------------
## LOAD PACKAGES
## -----------------------------------------------------------------------------

suppressPackageStartupMessages(library(GetoptLong))
suppressPackageStartupMessages(library(recipes))
suppressPackageStartupMessages(library(parsnip))
suppressPackageStartupMessages(library(dplyr))
GetoptLong.options(help_style = "two-column")

args = new.env()
args$VERSION = "0.1.0"

GetoptLong(
    "training_period=s{1}", "Period of training data.",
    "serving_period=s{1}", "Period of serving data.",
    envir = args
)

## -----------------------------------------------------------------------------
## MODEL FUNCTIONS
## -----------------------------------------------------------------------------


read_ews_data = function(period) {

    stopifnot(length(period) > 0)

    path = glue::glue(here::here("interim_data", "ews-{period}.rds"))
    stopifnot(
        "path to `period` does not exist." = file.exists(path)
    )
    out = readRDS(file = path)
    out
}

drop_indeterminate_group = function(ews_data) {

    stopifnot(is.data.frame(ews_data) && NROW(ews_data) > 0)

    out = ews_data %>%
        filter(quansat_3thang != "I") %>%
        mutate(quansat_3thang = forcats::fct_drop(quansat_3thang, only = "I")) %>%
        select(!matches("^bi_qh_gannhat") & !contains(c("quansat_2thang", "quansat_1thang", "customer_code",
                                                        "report_month", "short_name", "company_book", "company_name_vn")))
    out
}

drop_columns = function(ews_data) {

    stopifnot(is.data.frame(ews_data) && NROW(ews_data) > 0)
    out = ews_data %>%
        select(!matches("^bi_qh_gannhat") &
               !contains(c("quansat_2thang", "quansat_1thang", "customer_code",
                           "report_month", "short_name", "company_book", "company_name_vn")))
    out
}


## -----------------------------------------------------------------------------
## Data Pre-processing
## -----------------------------------------------------------------------------

feature_engineering = function(training_data,
                               serving_data,
                               purpose = c("training", "serving")) {

    stopifnot(is.data.frame(training_data) & NROW(training_data) > 0)
    stopifnot(is.data.frame(serving_data) & NROW(serving_data) > 0)

    purpose = match.arg(purpose)

    ## build feature engineering recipe
    recipe = training_data %>%
        recipe(quansat_3thang ~ .) %>%
        step_nzv(all_predictors()) %>%
        step_bagimpute(bi_age, bi_gender) %>%
        step_log(bi_sodu_3t, bi_sodu_6t, bi_sodu_9t, bi_sodu_12t,
                 bi_duno_3t, bi_duno_6t, bi_duno_9t, bi_duno_12t,
                 base = 10) %>%
        step_dummy(all_nominal(), -all_outcomes(), one_hot = TRUE) %>%
        step_corr(all_numeric(), -all_outcomes(), threshold = 0.8) %>%
        step_center(all_numeric(), -all_outcomes()) %>%
        step_scale(all_numeric(), -all_outcomes())

    ## actually processing data
    if (purpose == "training") {
        out = bake(prep(recipe), new_data = NULL)
    } else {
        out = bake(prep(recipe), new_data = serving_data)
    }

    out
}

## -----------------------------------------------------------------------------
## Model Training & Tuning
## -----------------------------------------------------------------------------

ranger_model_training = function(training_data) {

    stopifnot(is.data.frame(training_data) & NROW(training_data) > 0)

    ranger_learner = rand_forest(mode = "classification") %>%
        set_engine(engine = "ranger", importance = "permutation")
    ranger_trained = fit(ranger_learner, quansat_3thang ~ ., data = training_data)
    ranger_trained
}

ranger_model_prediction = function(trained_model, serving_data) {

    stopifnot(is.data.frame(serving_data) & NROW(serving_data) > 0)

    prediction = trained_model %>%
        predict(new_data = serving_data) %>%
        bind_cols(predict(trained_model, new_data = serving_data, type = "prob"))
    prediction
}

classify_color = function(prediction_data) {
    dt = prediction_data
    out = dt %>%
        arrange(desc(.pred_B)) %>%
        mutate(pct = 1:NROW(dt) / NROW(dt)) %>%
        mutate(grp = case_when(pct <= 0.12 ~ "Red",
                               pct > 0.12 & pct <= 0.57 ~ "Yellow",
                               TRUE ~ "Green")) %>%
        select(-pct)
    stopifnot(anyNA(out$grp) == FALSE)
    out
}

pipeline = function(training_period, serving_period) {

    ews_training = read_ews_data(period = training_period)
    ews_serving = read_ews_data(period = serving_period)

    ews_cus = ews_serving %>%
        select(all_of(c("company_book", "company_name_vn", "customer_code", "short_name")))

    ews_training = drop_indeterminate_group(ews_training)
    ews_serving = drop_columns(ews_serving)

    ews_training_fe = feature_engineering(ews_training, ews_serving, purpose = "training")
    ews_serving_fe = feature_engineering(ews_training, ews_serving, purpose = "serving")

    ews_model = ranger_model_training(ews_training_fe)
    ews_prediction = ranger_model_prediction(ews_model, ews_serving_fe)

    out = bind_cols(ews_cus, select(ews_prediction, .pred_B))
    out = classify_color(out)
    out = mutate(out, rowid = 1:NROW(out), .before = company_book)
    names(out) = c("STT", "MA CN", "TEN CN", "ID KH", "HO TEN KH", "PD", "PHAN HANG")
    out
}

ews_prediction = pipeline(training_period = args$training_period, serving_period = args$serving_period)
writexl::write_xlsx(ews_prediction, path = here::here(glue::glue("outputs/EWS_PREDICTION_{args$serving_period}.xlsx")))
