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
                      PWD = '2024#tpb',
                      Database = "DATA",
                      Port = 1433)


f_push_visa <- function(PROCESS_DATE, .path){
  
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
  
  
  file_name <- paste0(.path, '/VISA028_SV_', format(PROCESS_DATE, '%Y%m%d'), '_clean.csv')
  
  dt <- read_csv(file_name,
                 col_types = cols(
                   .default = col_character(),
                   X1 = col_double(),
                   CREDIT_LIMIT = col_double(),
                   OPENING_DATE = col_date(format = ""),
                   EXPIRE_DATE = col_character(),  # Change to character type
                   OD_COUNT_DAYS = col_double(),
                   Duno_saoke = col_double(),
                   Duno_vuotHM = col_double(),
                   Duno = col_double()
                 )) %>% 
    mutate(PROCESS_DATE = ymd(PROCESS_DATE),  # Assuming PROCESS_DATE is also a date column
           EXPIRE_DATE = case_when(
             grepl("/", EXPIRE_DATE) ~ dmy(EXPIRE_DATE),  # Format "31/12/2023"
             TRUE ~ ymd(EXPIRE_DATE)  # Format "2023-12-31"
           )) %>% 
    select(PROCESS_DATE, STT = X1, everything())
  dt <- dt[,-c(2,16,22,23,24,25,26,27,28,29,30)]

  fnc_push_db(dt, "VISA028_CLEAN", con_data )
  
  print(file_name)
  return(dt)
}


df <- f_push_visa(as.Date('2024-01-31'), 'Z:\\Noibo\\PHONG MHRR\\LanVH\\DATA\\01-2024')


tbl(con_data, 'VISA028_CLEAN') %>% 
  group_by(PROCESS_DATE) %>% 
  count() %>% 
  arrange(desc(PROCESS_DATE))



