#=============================================================================
# First: 2022-05
# Thay doi process_date
#=============================================================================
rm(list = ls())

library(DBI)
library(odbc)
library(tictoc)
library(tidyverse)
con_ews <- dbConnect(odbc(),
                     Driver = "SQL Server",
                     Server = "VM-DC-JUMPSRV77\\IFRS9", # chu y phai dung 2 dau gach ngang
                     # Database = "IFRS9_CUSTOMER",
                     Database = "EWS",
                     UID = "rdm_admin",
                     PWD = "2024#tpb",
                     Port = 1433)

# run_date = '2022-09-30'
run_date = '2024-06-30'
# run_date = readline('Enter run_date (yyyy-mm-dd): ')
#=============================================================================
ews_data_raw <- tbl(con_ews, 'EWS_PORTFOLIO_SHORTLIST_STORE') %>% 
  filter(process_date == run_date) %>% 
  collect() %>% 
  # select(-customer_type, -cg_segment, -ifrs9_segment, - mob) %>% 
  janitor::clean_names()



# Du lieu cham diem B-score
ews_data <- ews_data_raw %>% 
  filter(model == 'B-score')

ews_other_rating <- ews_data_raw %>% 
  filter(model != 'B-score')

# autoloan <- readRDS("1. Data/autoloan.rds")
# names(autoloan) %>% 
#   setdiff(names(ews_data)) %>% 
#   as.data.frame() %>% 
#   writexl::write_xlsx("x.xlsx")

ews_data <- ews_data %>%
  mutate(
    segment_odr_lv1=0,
    default_flag=0,
    max_dpd_current=0,
    customer_type=0,
    cg_segment=0,
    mth_in_rel=0,
    yr_in_rel=0,
    age=0,
    education=0,
    gender=0,
    marital_status=0,
    yr_in_bussiness=0,
    fs_asset=0,
    fs_sales=0,
    os_onbs=0,
    os_onbs_l3m=0,
    os_onbs_l6m=0,
    avg_os_onbs=0,
    avg_os_onbs_l3m=0,
    avg_os_onbs_l6m=0,
    os=0,
    os_l3m=0,
    os_l6m=0,
    os_non_revl=0,
    os_non_revl_l3m=0,
    os_revl_l6m=0,
    os_non_revl_l6m=0,
    month_on_book_cl=0,
    month_on_book_lc=0,
    month_on_book_bl=0,
    month_on_book_card=0,
    month_on_book_od=0,
    month_on_book_autoloan=0,
    month_on_book_mortgage=0,
    month_on_book=0,
    remain_term_cl=0,
    remain_term_lc=0,
    remain_term_bl=0,
    remain_term_card=0,
    remain_term_mortgage=0,
    cnt_mth_onbs_l3m=0,
    cnt_mth_onbs_l6m=0,
    os_amt_short_curr=0,
    os_amt_medium_curr=0,
    os_amt_short_curr_remain_term=0,
    os_amt_medium_curr_remain_term=0,
    os_amt_long_curr_remain_term=0,
    flag_foreign_c_loan_curr=0,
    ovd_amt_curr=0,
    flag_lc_used_curr=0,
    flag_lc_used_6m=0,
    flag_lc_used_3m=0,
    flag_bl_used_curr=0,
    flag_bl_used_6m=0,
    flag_bl_used_3m=0,
    flag_pay_od_curr=0,
    max_amt_used_od_6m=0,
    max_amt_used_od_3m=0,
    hav_tpbs_acc=0,
    use_pay_roll=0,
    td_bal=0,
    td_bal_l3m=0,
    td_bal_l6m=0,
    td_bal_os_onbs=0,
    td_bal_os_onbs_l3m=0,
    td_bal_os_onbs_l6m=0,
    td_bal_avg=0,
    td_bal_l3m_avg=0,
    td_bal_l6m_avg=0,
    td_bal_os_onbs_avg=0,
    td_bal_os_onbs_l3m_avg=0,
    td_bal_os_onbs_l6m_avg=0,
    td_bal_onl_avg=0,
    td_bal_legacy_avg=0,
    td_bal_onl_legacy_avg=0,
    td_bal_onl_avg_l3m=0,
    td_bal_legacy_avg_l3m=0,
    td_bal_onl_legacy_avg_l3m=0,
    td_bal_onl_avg_l6m=0,
    td_bal_legacy_avg_l6m=0,
    td_bal_onl_legacy_avg_l6m=0,
    td_bal_onl_legacy_os_onbs_avg=0,
    td_bal_onl_legacy_os_onbs_avg_l3m=0,
    td_bal_onl_legacy_os_onbs_avg_l6m=0,
    ca_bal_l6m=0,
    ca_bal_os_onbs=0,
    ca_bal_os_onbs_l3m=0,
    ca_bal_os_onbs_l6m=0,
    ca_bal_avg=0,
    ca_bal_l3m_avg=0,
    ca_bal_l6m_avg=0,
    ca_bal_os_onbs_l6m_avg=0,
    ca_td_bal_legacy_l3m=0,
    ca_td_bal_legacy_l6m=0,
    ca_td_bal_legacy_os_onbs=0,
    ca_td_bal_legacy_os_onbs_l3m=0,
    ca_td_bal_legacy_os_onbs_l6m=0,
    avg_ca_td_legacy_bal=0,
    avg_ca_td_bal_legacy_l3m=0,
    avg_ca_td_bal_legacy_l6m=0,
    avg_ca_td_legacy_bal_os_onbs=0,
    avg_ca_td_bal_legacy_os_onbs_l3m=0,
    avg_ca_td_bal_legacy_os_onbs_l6m=0,
    avg_ca_td_bal_legacy_l3m_vs_l6m=0,
    avg_ca_td_bal_legacy_vs_os_onbs_l3m=0,
    avg_ca_td_bal_legacy_vs_dbt_svc_l6m=0,
    total_bal=0,
    total_bal_os_onbs=0,
    total_bal_os_onbs_avg=0,
    bal_l3m=0,
    bal_os_onbs_l3m=0,
    bal_os_onbs_l6m=0,
    avg_bal_onbs_l3m=0,
    avg_txn_amt=0,
    avg_txn_amt_os_onbs=0,
    avg_txn_amt_l3m=0,
    avg_txn_amt_l6m=0,
    avg_txn_amt_os_onbs_l6m=0,
    avg_txn_amt_l1m_vs_l3m=0,
    avg_txn_amt_l1m_vs_l6m=0,
    cash_txn_amt=0,
    cash_txn_amt_l3m=0,
    cash_txn_amt_l6m=0,
    avg_cash_vs_txn_amt_l1m=0,
    avg_cash_vs_txn_amt_l3m=0,
    avg_cash_vs_txn_amt_l6m=0,
    avg_cash_txn_amt_per_time_l3m=0,
    avg_cash_txn_amt_per_time_l6m=0,
    ebank_txn_amt=0,
    avg_ebank_txn_amt_l3m=0,
    avg_sal_txn_amt_l3m=0,
    min_sal_txn_amt_l3m=0,
    avg_ebank_txn_amt_l6m=0,
    avg_sal_txn_amt_l6m=0,
    min_sal_txn_amt_l6m=0,
    cash_cr_txn_amt=0,
    cash_cr_txn_amt_l3m=0,
    cash_cr_txn_amt_l6m=0,
    cash_dr_txn_amt=0,
    cash_dr_txn_amt_l3m=0,
    cash_dr_txn_amt_l6m=0,
    cr_txn_amt=0,
    cr_txn_amt_l3m=0,
    cr_txn_amt_l6m=0,
    dr_txn_amt=0,
    dr_txn_amt_l3m=0,
    dr_txn_amt_l6m=0,
    cashout_3m_pct=0,
    net_flow_txn_amt=0,
    avg_cash_cr_txn_amt_covr_l3m=0,
    avg_cr_txn_amt_vs_os_l3m=0,
    max_net_flow_txn_amt_l6m=0,
    avg_cash_cr_txn_amt_covr_l6m=0,
    service_amt=0,
    min_service_amt_l3m=0,
    min_service_amt_l6m=0,
    num_cash_txn=0,
    num_non_cash_txn=0,
    avg_cash_txn=0,
    avg_non_cash_txn=0,
    total_num_cash_txn_l3m=0,
    total_num_non_cash_txn_l3m=0,
    avg_num_non_cash_txn_cvr_l3m=0,
    avg_cash_txn_l3m=0,
    total_num_cash_txn_l6m=0,
    total_num_non_cash_txn_l6m=0,
    avg_num_non_cash_txn_cvr_l6m=0,
    avg_cash_txn_l6m=0,
    flag_salary_l3m=0,
    coll_val=0,
    coll_val_ctr=0,
    avg_coll_l3m=0,
    avg_coll_ctr_l3m=0,
    avg_coll_l6m=0,
    avg_coll_ctr_l6m=0,
    coll_val_os_onbs=0,
    coll_val_ctr_os_onbs=0,
    coll_val_os_onbs_l3m=0,
    coll_val_os_onbs_l6m=0,
    avg_coll_covr_os_onbs_l3m=0,
    cust_lmt_util=0,
    coll_covr_lmt=0,
    total_cust_lmt=0,
    os_onbs_util=0,
    avg_os_onbs_util_l3m=0,
    avg_os_onbs_util_l6m=0,
    gr_os_onbs_util_l3m=0,
    gr_os_onbs_util_l6m=0,
    os_pct_od_curr=0,
    revl_lmt=0,
    os_lmt_revl=0,
    os_lmt_revl_l3m=0,
    os_lmt_revl_l6m=0,
    month_on_book_cc_6m=0,
    month_on_book_cc_3m=0,
    bal_at_cycle_end=0,
    bal_at_end_month=0,
    gross_payment_lagged=0,
    gross_payment_inmonth=0,
    gross_payment_lagged_1m=0,
    avg_cc_pmt_vs_stm_bal=0,
    avg_cc_pmt_vs_stm_bal_l3m=0,
    avg_cc_pmt_vs_stm_bal_l6m=0,
    credit_limit=0,
    bal_cr_limt=0,
    bal_cr_limt_l3m=0,
    bal_cr_limt_l6m=0,
    bal_end_cr_limt=0,
    bal_end_cr_limt_l3m=0,
    bal_end_cr_limt_l6m=0,
    gr_bal_cr_limt_l3m=0,
    gr_bal_cr_limt_l6m=0,
    gr_bal_end_cr_limt_l3m=0,
    gr_bal_end_cr_limt_l6m=0,
    num_10dpd=0,
    num_90dpd=0,
    ln_class_tpbank=0,
    dpd=0,
    num_xdpd_l6m=0,
    num_30dpd_l6m=0,
    mth_snc_b2x_l6m=0,
    mth_snc_xdpd_l6m=0,
    avg_util_cc_rate=0,
    avg_util_cc_rate_l3m=0,
    avg_util_cc_rate_l6m=0,
    gr_avg_util_cc_rate_l3m=0,
    gr_avg_util_cc_rate_l6m=0,
    nbr_txn_cc=0,
    nbr_txn_cc_l3m=0,
    nbr_txn_cc_l6m=0,
    nbr_cash_txn=0,
    nbr_cash_txn_l3m=0,
    nbr_cash_txn_l6m=0,
    cash_txn_rt=0,
    cash_txn_l3m_rt=0,
    cash_txn_l6m_rt=0,
    nbr_pos_txn=0,
    nbr_pos_txn_l3m=0,
    nbr_pos_txn_l6m=0,
    pos_txn_rt=0,
    pos_txn_l3m_rt=0,
    pos_txn_l6m_rt=0,
    nbr_onl_txn=0,
    nbr_onl_txn_l3m=0,
    nbr_onl_txn_l6m=0,
    onl_txn_rt=0,
    onl_txn_l3m_rt=0,
    onl_txn_l6m_rt=0,
    cash_amt_txn=0,
    onl_amt_txn=0,
    pos_amt_txn=0,
    cash_amt_txn_l3m=0,
    onl_amt_txn_l3m=0,
    pos_amt_txn_l3m=0,
    cash_amt_txn_l6m=0,
    onl_amt_txn_l6m=0,
    pos_amt_txn_l6m=0,
    mth_snc_10dpd_l6m=0,
    mth_snc_30dpd_l6m=0,
    mth_snc_60dpd_l6m=0,
    mth_snc_90dpd_l6m=0,
    num_tm_10dpd_l6m=0,
    num_tm_30dpd_l6m=0,
    num_tm_60dpd_l6m=0,
    num_tm_90dpd_l6m=0,
    avg_cr_txn_amt_vs_os=0,
    gr_cr_txn_amt_l3m=0,
    gr_cr_txn_amt_l6m=0,
    num_cl_10dpd_l3m=0,
    num_cl_10dpd_l6m=0,
    num_cl_30dpd_l3m=0,
    num_cl_30dpd_l6m=0,
    num_cl_60dpd_l3m=0,
    num_cl_60dpd_l6m=0,
    num_cl_90dpd_l3m=0,
    num_cl_90dpd_l6m=0,
    num_cus_10dpd_l3m=0,
    num_cus_10dpd_l6m=0,
    num_cus_30dpd_l3m=0,
    num_cus_30dpd_l6m=0,
    num_cus_60dpd_l3m=0,
    num_cus_60dpd_l6m=0,
    num_cus_90dpd_l3m=0,
    num_cus_90dpd_l6m=0
  )

# saveRDS(ews_data, file = "1. Data/ews_data.rds")
# saveRDS(ews_data, file = "C:/PD_Base (B-score)_2021.09.07/1. Autoloan/1. Data/ews_data_202110.rds")
# saveRDS(ews_data, file = "C:/PD_Base (B-score)_2021.09.07/1. Autoloan/1. Data/ews_data_202111.rds")
# saveRDS(ews_data, file = "C:/PD_Base (B-score)_2021.09.07/1. Autoloan/1. Data/ews_data_202112.rds")
# saveRDS(ews_data, file = "C:/PD_Base (B-score)_2021.09.07/1. Autoloan/1. Data/ews_data_202201.rds")
# saveRDS(ews_data, file = "C:/PD_Base (B-score)_2021.09.07/1. Autoloan/1. Data/ews_data_202202.rds")
# saveRDS(ews_data, file = "C:/PD_Base (B-score)_2021.09.07/1. Autoloan/1. Data/ews_data_202203.rds")
# saveRDS(ews_data, file = "C:/PD_Base (B-score)_2021.09.07/1. Autoloan/1. Data/ews_data_202205.rds")
# saveRDS(ews_data, file = "C:/PD_Base (B-score)_2021.09.07/1. Autoloan/1. Data/ews_data_202206.rds")
# saveRDS(ews_data, file = "C:/PD_Base (B-score)_2021.09.07/1. Autoloan/1. Data/ews_data_202207.rds")
# saveRDS(ews_data, file = "C:/PD_Base (B-score)_2021.09.07/1. Autoloan/1. Data/ews_data_202208.rds")

# ============================================================================
# Data for ews
# source('C:/PD_Base (B-score)_2021.09.07/1. Autoloan/2. Codes/_data_for_EWS.R')
library(lightgbm)
library(mlr3)
library(mlr3pipelines)
# library(mlr3extralearners)
library(mlr3filters)
library(mlr3learners.lightgbm)
library(tidyverse)
setwd('E:/PD_Base (B-score)_2021.09.07')
gl = drake::readd(glrn_classif_not_train_classif.lightgbm)$clone()

# gl = drake::readd(glrn_classif_classif.lightgbm)$clone()
# gl$param_set$values = drake::readd(tune_single_crit_classif.lightgbm)$result_learner_param_vals
gl$param_set$values$auc.filter.frac <- 0.443041592976078
gl$param_set$values$find_correlation.filter.frac <- 0.477305099647492
gl$param_set$values$classif.lightgbm.learning_rate <- 0.025
gl$param_set$values$classif.lightgbm.num_iterations <- 317


task <- drake::readd(task_classif)

# set seed thi glrn not train va glrn trained cho ra cung ket qua
set.seed(158)
gl$train(task)

master_scale_portfolio = readRDS("1. Data/master_scale.rds")


ews_prediction = gl$predict_newdata(select(ews_data, - mud_code, -product_modelling), task = task)


ews_this_month_portfolio = ews_data %>%
  bind_cols(as.data.table(ews_prediction)) %>%
  mutate(score = (1 - prob.bad) * 1000) %>%
  mutate(
    portfolio = case_when(
      product_modelling == 'Autoloan' ~ 'autoloan',
      product_modelling == 'Consumer lending - secured' ~ 'secured',
      product_modelling == 'Consumer lending - unsecured' ~ 'unsecured',
      product_modelling == 'Mortgage' ~ 'mortgage',
      product_modelling == 'Others' ~ 'others',
      TRUE ~ 'xxx'
    )
  ) %>%
  # filter(portfolio != 'xxx')
  inner_join(master_scale_portfolio, by = "portfolio") %>%
  filter(score > score_lower, score <= score_upper) %>%
  select(
    branch_code,
    portfolio,
    process_date,
    customer_id,
    score,
    grade,
    pd_12_month = ttc_pd,
    dpd_l6m,
    dpd_l3m,
    min_ca_bal_l6m,
    avg_total_num_txn_l6m,
    avg_total_num_txn,
    avg_total_num_txn_l3m,
    avg_cr_txn_amt_vs_os_l6m,
    avg_txn_amt_os_onbs_l3m,
    vol_os_l6m,
    remain_term_autoloan,
    max_os_onbs_l6m,
    avg_bal_onbs_l6m,
    remain_term,
    total_bal_avg,
    ca_bal_os_onbs_avg,
    total_num_txn_l6m,
    avg_bal_l3m,
    avg_bal_l6m,
    ca_bal_os_onbs_l3m_avg,
    bal_l6m,
    os_amt_long_curr,
    total_num_txn_l3m,
    max_net_flow_txn_amt_l3m,
    ca_bal_l3m,
    max_dpd_loan,
    total_num_txn,
    os_revl_l3m,
    os_revl,
    ca_bal,
    min_day_to_matdt,
    num_cr_product_l3m,
    ca_td_bal_legacy,
    grade_num,
    bucket,
    model,
    mob_portfolio,
    max_dpd_portfolio,
    deal_sub_type,
    current_default
  )

# ============================================================================
# grade khong theo portfolio
master_scale_ews <- readRDS("1. Data/master_scale_ews.rds")

ews_this_month = ews_data %>%
  bind_cols(as.data.table(ews_prediction)) %>%
  mutate(score = (1 - prob.bad) * 1000) %>%
  # cross-join
  inner_join(master_scale_ews, by = character()) %>%
  filter(score > score_lower, score <= score_upper) %>%
  select(
    process_date,
    customer_id,
    score,
    grade,
    pd_12_month = ttc_pd,
    dpd_l6m,
    dpd_l3m,
    min_ca_bal_l6m,
    avg_total_num_txn_l6m,
    avg_total_num_txn,
    avg_total_num_txn_l3m,
    avg_cr_txn_amt_vs_os_l6m,
    avg_txn_amt_os_onbs_l3m,
    vol_os_l6m,
    remain_term_autoloan,
    max_os_onbs_l6m,
    avg_bal_onbs_l6m,
    remain_term,
    total_bal_avg,
    ca_bal_os_onbs_avg,
    total_num_txn_l6m,
    avg_bal_l3m,
    avg_bal_l6m,
    ca_bal_os_onbs_l3m_avg,
    bal_l6m,
    os_amt_long_curr,
    total_num_txn_l3m,
    max_net_flow_txn_amt_l3m,
    ca_bal_l3m,
    max_dpd_loan,
    total_num_txn,
    os_revl_l3m,
    os_revl,
    ca_bal,
    min_day_to_matdt,
    num_cr_product_l3m,
    ca_td_bal_legacy,
    grade_num,
    bucket,
    model
  ) %>%
  distinct()
# ============================================================================
ews_other_portfolio_rating <- ews_other_rating %>% 
  mutate(
    portfolio = case_when(
      product_modelling == 'Autoloan' ~ 'autoloan',
      product_modelling == 'Consumer lending - secured' ~ 'secured',
      product_modelling == 'Consumer lending - unsecured' ~ 'unsecured',
      product_modelling == 'Mortgage' ~ 'mortgage',
      product_modelling == 'Others' ~ 'others',
      TRUE ~ 'xxx'
    )
  ) %>% 
  mutate(grade = case_when(bucket == 'Bucket 5' ~ 'Default',
                           bucket == 'Bucket 1: MOB < 6 and DPD 0 - 30' ~ 'D0',
                           bucket == 'Bucket 3: DPD 31 - 60' ~ 'D1',
                           bucket == 'Bucket 4' ~ 'D2',
                           bucket == 'Bucket 2: MOB >= 6 and DPD 0 - 30' ~ 'error'
  ),
  grade_num = case_when(grade == 'Default' ~ 13,
                        grade == 'D0' ~ 0,
                        grade == 'D1' ~ 11,
                        grade == 'D2' ~ 12
  )) %>% 
  mutate(score = NA_real_,
         pd_12_month = NA_real_) %>% 
  dplyr::select(
    process_date,
    customer_id,
    score,
    grade,
    pd_12_month,
    dpd_l6m,
    dpd_l3m,
    min_ca_bal_l6m,
    avg_total_num_txn_l6m,
    avg_total_num_txn,
    avg_total_num_txn_l3m,
    avg_cr_txn_amt_vs_os_l6m,
    avg_txn_amt_os_onbs_l3m,
    vol_os_l6m,
    remain_term_autoloan,
    max_os_onbs_l6m,
    avg_bal_onbs_l6m,
    remain_term,
    total_bal_avg,
    ca_bal_os_onbs_avg,
    total_num_txn_l6m,
    avg_bal_l3m,
    avg_bal_l6m,
    ca_bal_os_onbs_l3m_avg,
    bal_l6m,
    os_amt_long_curr,
    total_num_txn_l3m,
    max_net_flow_txn_amt_l3m,
    ca_bal_l3m,
    max_dpd_loan,
    total_num_txn,
    os_revl_l3m,
    os_revl,
    ca_bal,
    min_day_to_matdt,
    num_cr_product_l3m,
    ca_td_bal_legacy,
    grade_num,
    bucket,
    model
  ) %>%
  distinct()


# ============================================================================


# Day len database

f_push_db <- function(df, sql_tbl_name) {
  
  tic(paste("Total time", sql_tbl_name))
  
  # divide data if necessary
  n <- nrow(df)
  chunk <- 10500
  r <- rep(1:ceiling(n/chunk), each = chunk)
  d <- split(df, r)
  
  # Push to DB
  tic("Time to push into DB")
  lapply(d, dbWriteTable, conn = con_ews, name = sql_tbl_name, append = TRUE)
  toc()
  
  toc()
}

f_push_db(ews_this_month_portfolio, 'ews_khcn_portfolio_score_store')
f_push_db(ews_other_portfolio_rating, 'ews_khcn_portfolio_score_store')

f_push_db(ews_this_month, 'ews_khcn_customer_score_store')



list(
  ews_by_portfolio = ews_this_month_portfolio,
  ews_customer = ews_this_month
) %>% 
  writexl::write_xlsx(paste0("5. Report/ews/ews_",format(as.Date(run_date), '%Y%m'),"_features.xlsx"))



#=============================================================================

# PSI function
fn_psi <- function(actual, expected) {
  percent_actual = actual / sum(actual, na.rm = T)
  percent_expected = expected / sum(expected, na.rm = T)
  psi = (percent_actual - percent_expected) * (log(percent_actual/percent_expected))
  return(round(psi, 4))
}

# lastmonth
library(lubridate)
prev_date <- (as.Date(run_date) %m-% months(1)) %>% 
  ceiling_date(unit = 'months') %>% 
  magrittr::subtract(1) %>% 
  as.Date() %>% 
  as.character()

prev_date

# get data customer from DB
ews_khcn_customer <- tbl(con_ews, 'ews_khcn_customer_score_store') %>% 
  filter(process_date %in% c(run_date, prev_date)) %>% 
  collect() %>% 
  janitor::clean_names() %>% 
  group_by(process_date, grade) %>% 
  count() %>% 
  mutate(process_date = format(as.Date(process_date), '%Y%m')) %>% 
  mutate(grade = factor(grade),
         grade = fct_relevel(grade, 'AAA', 'AA', 'A', 'BBB', 'BB', 'B', 'CCC', 'CC', 'C', 'D')) %>% 
  pivot_wider(names_from = 'process_date',values_from = n) %>% 
  arrange(grade)

ews_khcn_customer['last_psi'] <- fn_psi(ews_khcn_customer[,2],ews_khcn_customer[,3]) * 100

ews_khcn_customer

ews_khcn_customer %>% 
  pivot_longer(cols = c(2,3)) %>% 
  ggplot(aes(grade, value))+
  geom_col(aes(fill = name, color = name), position = position_dodge(), alpha = 0.4)+
  labs(title = 'Stability - Month on Month',
       y = 'number of customer')+
  theme_bw()+
  theme(legend.position = 'bottom',
        legend.title = element_blank())

# ============================================================================
# get data portfolio from DB

ews_khcn_portfolio <- tbl(con_ews, 'ews_khcn_portfolio_score_store') %>% 
  filter(process_date %in% c(run_date, prev_date)) %>% 
  collect() %>% 
  janitor::clean_names() %>% 
  group_by(process_date, portfolio, grade) %>% 
  count() %>% 
  mutate(process_date = format(as.Date(process_date), '%Y%m')) %>% 
  mutate(grade = factor(grade),
         grade = fct_relevel(grade, 'AAA', 'AA', 'A', 'BBB', 'BB', 'B', 'CCC', 'CC', 'C', 'D')) %>% 
  pivot_wider(names_from = 'process_date',values_from = n) %>% 
  arrange(grade)

f_graph_portfolio <- function(.portfolio){
  
  grade_tbl_port <- ews_khcn_portfolio %>% 
    filter(portfolio == .portfolio)
  
  grade_tbl_port['last_psi'] <- fn_psi(grade_tbl_port[,3],grade_tbl_port[,4]) * 100
  
  p <- grade_tbl_port %>% 
    pivot_longer(cols = c(3,4)) %>% 
    ggplot(aes(grade, value))+
    geom_col(aes(fill = name, color = name), position = position_dodge(), alpha = 0.4)+
    labs(title = 'Stability - Month on Month',
         subtitle = .portfolio,
         y = 'number of customer')+
    theme_bw()+
    theme(legend.position = 'bottom',
          legend.title = element_blank())
  p
}


f_graph_portfolio('autoloan')
f_graph_portfolio('mortgage')
f_graph_portfolio('others')
f_graph_portfolio('secured')
f_graph_portfolio('unsecured')
# ============================================================================
# process_date_in <- as.Date('2013-12-31') %m+% years(0:6)
# customer_rating <- tbl(con_ews, 'CUSTOMER_RATING') %>% 
#   filter(process_date %in% process_date_in) %>% 
#   collect() %>% 
#   janitor::clean_names()


# ============================================================================
ews_this_month_portfolio %>%
  filter(customer_id == '01877484')

ews_this_month %>%
  filter(customer_id == '01877484')
