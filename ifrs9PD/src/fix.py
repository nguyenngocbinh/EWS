import pandas as pd
import numpy as np

def fix_ews_khcn(df, master_scale):
    # Define the conditions and corresponding grades
    conditions_grade = [        
        (df['bucket'] == 'B1: MOB <6 & DPD 0-30'),
        (df['bucket'] == 'B3: DPD 31-60'),
        (df['bucket'] == 'B4: DPD 61-90'),
        (df['bucket'] == 'B5: DPD >90')
    ]
    grades = ['D0', 'D1', 'D2', 'Default']

    # Apply conditions using numpy.select for grade
    df['grade'] = np.select(conditions_grade, grades, default=df['grade'])

    # Define the conditions and corresponding models
    conditions_model = [
        (df['bucket'] == 'B2: MOB >=6 & DPD 0-30'),
        (df['bucket'].isin(['B1: MOB <6 & DPD 0-30', 'B3: DPD 31-60', 'B4: DPD 61-90'])),
        (df['bucket'] == 'B5: DPD >90')
    ]
    models = ['B-score', 'ODR LRA', 'Default']

    # Apply conditions using numpy.select for model
    df['model'] = np.select(conditions_model, models, default=df['model'])
    
    # Set score to None if bucket is not 'B2: MOB >=6 & DPD 0-30'
    df.loc[df['bucket'] != 'B2: MOB >=6 & DPD 0-30', 'score'] = None

    # Drop grade_num and ttc_pd columns
    df = df.drop(['grade_num', 'ttc_pd'], axis=1)

    # Merge with master_scale on grade and portfolio, and process_date between eff_date and exp_date
    merged_df = pd.merge(df, master_scale[['grade', 'portfolio', 'grade_num', 'ttc_pd']], on=['grade', 'portfolio'])

    # Update grade_num to 13 where grade is 'Default'
    merged_df.loc[merged_df['grade'] == 'Default', 'grade_num'] = 13

    # Set grade_num to NULL where grade is NULL
    merged_df.loc[merged_df['grade'].isnull(), 'grade_num'] = None

    # Set ttc_pd to NULL where grade is NULL
    merged_df.loc[merged_df['grade'].isnull(), 'ttc_pd'] = None

    # Set ttc_pd to 1 where grade is 'Default'
    merged_df.loc[merged_df['grade'] == 'Default', 'ttc_pd'] = 1

    return merged_df