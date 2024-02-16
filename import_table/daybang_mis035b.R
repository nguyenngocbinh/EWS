rm(list = ls())
library(odbc)
library(DBI)
library(dbplyr)
library(stringr)
library(lubridate)
library(readxl)
library(tidyverse)

# Connect database
con_data <- dbConnect(odbc(),
                      Driver = "SQL Server",
                      Server = "VM-DC-JUMPSRV77\\IFRS9", 
                      UID = 'rdm_admin',
                      PWD = 'Tpbank@2023',
                      Database = "DATA",
                      Port = 1433)

# update 2022-05-12
fnc_push_mis035b <- function(.path) {
  
  fnc_push_db <- function(df, sql_tbl_name, con = NULL) {
    
    tictoc::tic(paste("Total time", sql_tbl_name))
    
    # divide data if necessary
    n <- nrow(df)
    chunk <- 10000
    r <- rep(1:ceiling(n/chunk), each = chunk)
    d <- split(df, r)
    
    # Push to DB
    tictoc::tic("Time to push into DB")
    lapply(d, dbWriteTable, conn = con, name = sql_tbl_name, append = TRUE)
    tictoc::toc()
    
    tictoc::toc()
  }
  
  print('READING DATA ...')
  mis035b <- read_csv(.path,
                      col_types = cols(
                        .default = col_character(),
                        BACKUP_DATE = col_date(format = ""),
                        ACY_CURR_BALANCE = col_double(),
                        LCY_CURR_BALANCE = col_double(),
                        VALUE_DATE = col_date(format = ""),
                        MATURITY_DATE = col_date(format = ""),
                        RATE = col_double(),
                        TOD_END_DATE = col_date(format = ""),
                        MIN_INT_REVN_DATE = col_date(format = ""),
                        CHARGE_RATE = col_double()
                      )) %>%
    mutate_at(
      vars(
        "BRANCH_CODE",
        "CIF",
        "CUST_AC_NO",
        "GLCODE",
        "CCY",
        "CUSTOMER_TYPE",
        "CUST_CLASSIFICATION",
        "CUSTOMER_NAME1",
        "CUSTOMER_CATEGORY",
        "LOAITCTD",
        "PRODUCT",
        "NHOM_NO",
        "MA_CHUONG_TRINH",
        "AC1_SEGMT",
        "AC1_SEGMT_DESC",
        "NGUOI_GIOI_THIEU" ,
        "MIS_CODE",
        "CODE_DESC"   ,
        "SAN_PHAM_CU_THE"
      ),
      stringr::str_replace_all,
      pattern = '"="',
      replacement = ''
    ) %>%
    set_names(
      c(
        "PROCESS_DATE",
        "BRANCH_CODE",
        "CIF"      ,
        "CUST_AC_NO"      ,
        "GLCODE"      ,
        "CCY"      ,
        "ACY_CURR_BALANCE" ,
        "LCY_CURR_BALANCE"  ,
        "VALUE_DATE"    ,
        "MATURITY_DATE"      ,
        "CUSTOMER_TYPE"     ,
        "CUST_CLASSIFICATION",
        "CUSTOMER_NAME1" ,
        "CUSTOMER_CATEGORY"  ,
        "LOAITCTD"   ,
        "RATE"    ,
        "TOD_END_DATE",
        "MIN_INT_REVN_DATE" ,
        "PRODUCT"    ,
        "NHOM_NO" ,
        "MA_CHUONG_TRINH" ,
        "AC1_SEGMT"  ,
        "AC1_SEGMT_DESC"  ,
        "NGUOI_GIOI_THIEU"   ,
        "MIS_CODE" ,
        "CODE_DESC" ,
        "SAN_PHAM_CU_THE" ,
        "CHARGE_RATE"
      )
    )
  
  fnc_push_db(mis035b, "MIS035B", con_data )
  
  return(mis035b)
  
}

# ============================================================================
# note: change pattern = '=' or '"="'

fnc_push_mis035b("Z:\\Noibo\\PHONG MHRR\\LanVH\\DATA\\11-2023\\MIS035B_04122023110553.csv") -> dfx

tbl(con_data, 'MIS035B') %>% 
  group_by(PROCESS_DATE) %>% 
  count() %>% 
  arrange(desc(PROCESS_DATE))

tbl(con_data, 'MIS035B') %>% 
  head()
