# Author: BinhNN
# DAY BANG CL020
# First: 2021-12-03

rm(list = ls())
library(odbc)
library(DBI)
library(tidyverse)
library(dbplyr)
library(lubridate)
library(readxl)

con_data <- dbConnect(odbc(),
                      Driver = "SQL Server",
                      Server = "VM-DC-JUMPSRV77\\IFRS9", 
                      UID = 'rdm_admin',
                      PWD = 'Tpbank@2023',
                      Database = "DATA",
                      Port = 1433)


f_push_cl <- function(PROCESS_DATE, .path){
  
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
  
  cl020 <- read_excel(
    .path,
    skip = 7,
    col_types = c(
      rep('text', 8),
      'numeric',
      rep('date', 2),
      rep('numeric', 3),
      'text',
      'numeric',
      'text',
      'numeric',
      'date',
      'numeric',
      'date',
      rep('text', 7),
      'text',
      rep('text', 2)
    )
  ) %>%
    set_names(
      c(
        'BRANCH_CODE',
        'CUSTOMER_ID',
        'CUSTOMER_NAME',
        'LIMIT_ID',
        'CUSTOMER_TYPE',
        'CUSTOMER_GROUP',
        'CONTRACT_ID',
        'CCY',
        'DISBURSEMENT_AMOUNT',
        'DISBURSEMENT_DATE',
        'EXPIRY_DATE',
        'OS_AMOUNT',
        'EXCHANGE_RATE',
        'LCY_OS_AMOUNT',
        'NHOM_NO_CONTRACT',
        'OD_COUNT_DAY',
        'TT_QUA_HAN',
        'OD_PRINCIPLE',
        'FIRST_PRIN_OVD_DATE',
        'OD_INTEREST',
        'FIRST_INT_OVD_DATE',
        'MA_CVKH',
        'TEN_CVKH',
        'MA_CVTD',
        'TEN_CVTD',
        'BLANK',
        'MA_CGPD',
        'TEN_CGPD',
        'APPROVAL_DATE',
        'STATUS',
        'DETAIL_PRODUCT'
      )
    ) %>%
    mutate(PROCESS_DATE = PROCESS_DATE) %>%
    select(PROCESS_DATE, everything()) %>%
    select(-BLANK) %>%
    filter(!is.na(CUSTOMER_ID)) %>%
    filter(CUSTOMER_ID != 'Mã KH') %>%
    mutate(APPROVAL_DATE = lubridate::dmy(APPROVAL_DATE)) %>%
   # mutate(APPROVAL_DATE = as.Date(APPROVAL_DATE))
    # mutate_at(
    #   c(
    #     'DISBURSEMENT_DATE',
    #     'EXPIRY_DATE',
    #     'FIRST_PRIN_OVD_DATE',
    #     'FIRST_INT_OVD_DATE',
    #     'APPROVAL_DATE'
    #   ),
    #   lubridate::dmy
    # )# %>%
    mutate_at(
      c(
        'DISBURSEMENT_DATE',
        'EXPIRY_DATE',
        'FIRST_PRIN_OVD_DATE',
        'FIRST_INT_OVD_DATE',
        'APPROVAL_DATE'
      ),
      as.Date
    )
  fnc_push_db(cl020, "CL020", con_data )
  
  print(PROCESS_DATE)
  print(warnings())
  return(cl020)
}

# change here ---------------------
PROCESS_DATE = as.Date('2023-11-30')
# ---------------------------------


f_push_cl(PROCESS_DATE, 'Z:\\Noibo\\PHONG MHRR\\LanVH\\DATA\\11-2023\\CL020_DR_20231130.xlsx') -> dfx



tbl(con_data, 'CL020') %>% 
  group_by(PROCESS_DATE) %>% 
  count() %>% 
  arrange(desc(PROCESS_DATE))

