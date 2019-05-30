#!/usr/bin/env python
# coding: utf-8

# Separately run referrals and claims queries.
# This file imports the results of those queries, cleans and standardizes each, and merges them for analysis.

# In[ ]:


import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import pyodbc
import os


# ## Import Data

# In[ ]:


# with open('referrals.sql', 'r') as myfile:
#     cpts_sql_str=myfile.read().replace('\n', ' ')

# cnxn_cpts = pyodbc.connect('DRIVER={SQL Server};SERVER=colo-dwrpt01;DATABASE=IADS_V3')

# cpts = pd.read_sql(cpts_sql_str, cnxn_cpts)

# cnxn_cpts.close()


# In[ ]:


# cpts.to_csv('../Data/cpts_raw.csv', sep='|')


# In[ ]:


# with open('claims.sql', 'r') as myfile:
#     claims_sql_str=myfile.read().replace('\n', ' ')

# cnxn_claims = pyodbc.connect('DRIVER={SQL Server};SERVER=colo-dwrpt01;DATABASE=NATIONAL_ANALYTICS')

# claims = pd.read_sql(claims_sql_str, cnxn_claims)

# cnxn_claims.close()


# In[ ]:


# claims.to_csv('../Data/claims_raw.csv', sep='|')


# In[ ]:


# with open('referrals_new.sql', 'r') as myfile:
#     cpts_new_sql_str=myfile.read().replace('\n', ' ')

# cnxn_cpts_new = pyodbc.connect('DRIVER={SQL Server};SERVER=colo-dwrpt01;DATABASE=IADS_V3')

# cpts_new = pd.read_sql(cpts_new_sql_str, cnxn_cpts_new)

# cnxn_cpts_new.close()


# In[ ]:


# cpts_new.to_csv('../Data/cpts_new_raw.csv', sep='|')


# In[ ]:


# with open('cpt_desc.sql', 'r') as myfile:
#     cpt_desc_sql_str=myfile.read().replace('\n', ' ')

# cnxn_cpt_desc = pyodbc.connect('DRIVER={SQL Server};SERVER=colo-dwrpt01;DATABASE=IADS_V3')

# cpt_desc = pd.read_sql(cpt_desc_sql_str, cnxn_cpt_desc)

# cnxn_cpt_desc.close()


# In[ ]:


# cpt_desc.to_csv('../Data/cpt_desc_raw.csv', sep='|')


# In[ ]:


clinical_decisions_20190503_remove = pd.read_csv('../data/clinical_decision_20190503_remove.csv')


# In[ ]:


clinical_decisions_20190503_add = pd.read_csv('../data/clinical_decision_20190503_add.csv')


# In[ ]:


clinical_decisions_20190517 = pd.read_csv('../data/clinical_decision_20190517.csv')


# In[ ]:


clinical_decisions_20190522 = pd.read_csv('../data/clinical_decision_20190522.csv')


# In[ ]:


cpts = pd.read_csv('../Data/cpts_raw.csv', sep='|')


# In[ ]:


cpts.drop(['Unnamed: 0'], inplace=True, axis=1)


# In[ ]:


claims = pd.read_csv('../Data/claims_raw.csv', sep='|')


# In[ ]:


claims.drop(['Unnamed: 0'], inplace=True, axis=1)


# In[ ]:


cpts_new = pd.read_csv('../Data/cpts_new_raw.csv', sep='|')


# In[ ]:


cpts_new.drop(['Unnamed: 0'], inplace=True, axis=1)


# In[ ]:


cpt_desc = pd.read_csv('../Data/cpt_desc_raw.csv', sep='|')


# In[ ]:


cpt_desc.drop(['Unnamed: 0'], inplace=True, axis=1)


# ## Clean cpts

# In[ ]:


list_o_specs_original = list(set(cpts['Specialty'].unique().tolist() + cpts_new['Specialty'].unique().tolist()))


# In[ ]:


cpts_head = cpts.drop_duplicates(subset='HCP_CONNECT_AUTH_NUMBER').reset_index()

cpts_head.drop(columns = ['index', 'CPT_Code', 'Date_Decision',
       'Date_Received', 'ref_type', 'region', 'UNITS'], inplace=True)

cpts_head = cpts_head[cpts_head['HCP_CONNECT_AUTH_NUMBER'].isna()==False].reset_index()


# In[ ]:


cpts['Date_Decision'] = pd.to_datetime(cpts['Date_Decision'])


# In[ ]:


cpts['Date_Received'] = pd.to_datetime(cpts['Date_Received'])


# In[ ]:


cpts['LOB'].isna().sum()


# In[ ]:


new_lob = {'COMMERCIAL': '_b',
               'SENIOR': '_s',
          'MEDI-CAL': '_s'}
cpts['LOB'] = cpts['LOB'].replace(new_lob)


# In[ ]:


## Define a list of specialties that will be broken out into LOB for the purposes of AA
specs_w_lob_distinct = ['RADIOLOGY']


# In[ ]:


for spec in list_o_specs_original:
    if spec in specs_w_lob_distinct:
        cpts['Specialty'] = np.where(cpts['Specialty']==spec, cpts['Specialty']+cpts['LOB'], cpts['Specialty'])
    


# ## Clean Claims

# In[ ]:


## Claims sum is used for the cost of a cpt code when it doesn't exist in claims for the associated specialty
claims_sum = claims.groupby(['CPT_Code'], as_index=False).agg({'avg_hcp_cost': 'mean'})


# ## Clean Clinical Decisions

# In[ ]:


clinical_decisions_20190503_remove['Notes'].unique()


# In[ ]:


new_reason = {' Capped Providers/Employed ': 'capped providers',
               'Concern for Over-Utilization': 'overutilization concern',
               'Cost  containment with this speciality': 'cost containment',
               'drug?': 'UD',
               'Facility Code': 'facility code',
               'GUIDELINE REQ': 'guideline req', 
               'Heavy cap volume': 'heavy cap vol', 
               'Inappropriate location': 'inappropriate loc',
               'NEEDS BENEFIT CHECK': 'bene check', 
               'Needs Review': 'needs review', 
               'needs review': 'needs review',
               'Not CMS benefit': 'not CMS bene',
               'ok to AA to employed/cap providers. R1,II,III,VI': 'can approve',
               'Old code': 'old code',
               'RNL Stauts, intentionally surpressed, not currently reviewed; additional discussion': 'RNL',
               'Unclassified Drugs': 'unclassified drug', 
               'UPCODING': 'upcode'}
clinical_decisions_20190503_remove['Notes'] = clinical_decisions_20190503_remove['Notes'].replace(new_reason)
clinical_decisions_20190503_add['Notes'] = clinical_decisions_20190503_add['Notes'].replace(new_reason)


# ## Feature Engineering

# In[ ]:


## flag retro statuses with 1 and 0
retro_conditions = [
 (cpts['status_name'] == 'APPROVED - RETRO REVIEW') |
 (cpts['status_name'] == 'DENIED - RETRO REVIEW') |
 (cpts['status_name'] == 'APPROVED - COB RETRO') |
 (cpts['status_name'] == 'PENDING - RETRO REVIEW') 
  ]


# In[ ]:


## Create Refs - the older set of referrals from which we make the rules


# In[ ]:


choices = [1]
cpts['is_retro'] = np.select(retro_conditions, choices, default=0)


# In[ ]:


## remove retros from list and drop 'is_retro' as it is no longer needed
cpts = cpts[cpts['is_retro']==0]

cpts.drop(columns='is_retro', inplace=True)


# In[ ]:


cpts_det = cpts

cpts_det.drop(columns = ['PPL', 'Date_Decision',
       'Date_Received', 'ref_type', 'status_cat', 'status_name', 'Specialty',
       'region', 'UNITS'])

cpts_det = cpts_det[cpts_det['HCP_CONNECT_AUTH_NUMBER'].isna()==False].reset_index()


# In[ ]:


cpts['is_autoapp'] = np.where(cpts['status_name']=='APPROVED - AUTO', 1, 0)


# In[ ]:


cpts.PPL.fillna("N", inplace=True)


# In[ ]:


cpts['is_PPL'] = np.where(cpts['PPL']=='Y', 1, 0)


# In[ ]:


den_conditions = [
 (cpts['status_name'] == 'DENIED - CM') |
 (cpts['status_name'] == 'DENIED - BENEFIT CARVE OUT') |
 (cpts['status_name'] == 'DENIED - NOT A COVERED BENEFIT') |
 (cpts['status_name'] == 'DENIED - APPEAL') |
 (cpts['status_name'] == 'DENIED - CLINICAL TRIAL/EXP/INV') |
 (cpts['status_name'] == 'DENIED - TRANSPLANT') |
 (cpts['status_name'] == 'DENIED - MD') |
 (cpts['status_name'] == 'DENIED - CM/MD') |
 (cpts['status_name'] == 'DENIED - REDIRECT OSVN') |
 (cpts['status_name'] == 'DENIED - TICKLER')
  ]


# In[ ]:


choices = [1]
cpts['is_den'] = np.select(den_conditions, choices, default=0)


# In[ ]:


cpts['is_app'] = np.where(cpts['status_cat']=='APPROVED', 1, 0)


# ## Clean cpts_new - a new set of referrals to apply the rules to.

# In[ ]:


cpts_new['Date_Decision'] = pd.to_datetime(cpts_new['Date_Decision'])


# In[ ]:


cpts_new['Date_Received'] = pd.to_datetime(cpts_new['Date_Received'])


# In[ ]:


new_lob = {'COMMERCIAL': '_b',
               'SENIOR': '_s',
          'MEDI-CAL': '_s'}
cpts_new['LOB'] = cpts_new['LOB'].replace(new_lob)


# In[ ]:


for spec in list_o_specs_original:
    if spec in specs_w_lob_distinct:
        cpts_new['Specialty'] = np.where(cpts_new['Specialty']==spec, cpts_new['Specialty']+cpts_new['LOB'], cpts_new['Specialty'])
    


# In[ ]:


## flag retro statuses with 1 and 0
retro_conditions_new = [
 (cpts_new['status_name'] == 'APPROVED - RETRO REVIEW') |
 (cpts_new['status_name'] == 'DENIED - RETRO REVIEW') |
 (cpts_new['status_name'] == 'APPROVED - COB RETRO') |
 (cpts_new['status_name'] == 'PENDING - RETRO REVIEW') 
  ]


# In[ ]:


choices = [1]
cpts_new['is_retro'] = np.select(retro_conditions_new, choices, default=0)


# In[ ]:


## remove retros from list and drop 'is_retro' as it is no longer needed
cpts_new = cpts_new[cpts_new['is_retro']==0]

cpts_new.drop(columns='is_retro', inplace=True)


# In[ ]:


cpts_new['is_autoapp'] = np.where(cpts_new['status_name']=='APPROVED - AUTO', 1, 0)


# In[ ]:


cpts_new.PPL.fillna("N", inplace=True)


# In[ ]:


cpts_new['is_PPL'] = np.where(cpts_new['PPL']=='Y', 1, 0)


# In[ ]:


cpts_new['is_app'] = np.where(cpts_new['status_cat']=='APPROVED', 1, 0)


# In[ ]:


cpts_new['UNITS'] = 1


# In[ ]:


den_conditions_new = [
 (cpts_new['status_name'] == 'DENIED - CM') |
 (cpts_new['status_name'] == 'DENIED - BENEFIT CARVE OUT') |
 (cpts_new['status_name'] == 'DENIED - NOT A COVERED BENEFIT') |
 (cpts_new['status_name'] == 'DENIED - APPEAL') |
 (cpts_new['status_name'] == 'DENIED - CLINICAL TRIAL/EXP/INV') |
 (cpts_new['status_name'] == 'DENIED - TRANSPLANT') |
 (cpts_new['status_name'] == 'DENIED - MD') |
 (cpts_new['status_name'] == 'DENIED - CM/MD') |
 (cpts_new['status_name'] == 'DENIED - REDIRECT OSVN') |
 (cpts_new['status_name'] == 'DENIED - TICKLER')
  ]


# In[ ]:


choices = [1]
cpts_new['is_den'] = np.select(den_conditions_new, choices, default=0)


# In[ ]:


cpts_new = cpts_new[cpts_new['HCP_CONNECT_AUTH_NUMBER'].isna()==False].reset_index()


# In[ ]:


cpts_new.pivot_table(values='HCP_CONNECT_AUTH_NUMBER', index=['is_autoapp', 'is_den'], aggfunc='count', margins=True)


# In[ ]:


cpts_new.drop(labels=['index', 'REF_TYPE_KEY', 'PPL', 'Date_Received', 'Date_Decision', 'status_cat'], axis=1, inplace=True)


# In[ ]:


list_o_specs = list(set(cpts['Specialty'].unique().tolist() + cpts_new['Specialty'].unique().tolist()))


# ## Switch Point - old referrals vs new referrals

# In[ ]:


#cpts_new = cpts


# ## Create cpts_manual

# In[ ]:


## This is a cpt_code level list of all manually reviewed referrals


# In[ ]:


cpts_manual = cpts[cpts['is_autoapp']==0]


# In[ ]:


cpts_manual.pivot_table(values='HCP_CONNECT_AUTH_NUMBER', index=['is_autoapp', 'is_den'], aggfunc='count', margins=True)


# In[ ]:


## Calculate Cost per manually reviewed CPT code

ga_cpt = 6500000 / cpts[cpts['is_autoapp']==0].shape[0]


# In[ ]:


ga_cpt = 2.53


# In[ ]:


## This is a grouped list of refs_manual that summarizes the volume of manually reviewed cpt_codes


# In[ ]:


cpts_manual = cpts_manual.groupby(['Specialty', 'CPT_Code', 'is_PPL'], as_index=False).agg({
    'UNITS' : 'count'
})


# In[ ]:


cpts_manual.rename(index=str, columns={'UNITS': 'UNITS_man'}, inplace=True)


# In[ ]:


cpts_manual['cost_to_review'] = cpts_manual['UNITS_man']*ga_cpt


# ## Create cptssum

# In[ ]:


cptssum = cpts.groupby(['Specialty', 'CPT_Code', 'is_PPL'], as_index=False).agg({
    'UNITS': 'count',
    'is_autoapp': 'mean',
    'is_den': 'mean',
})


# In[ ]:


cpts_w_claims0 = pd.merge(cptssum, cpts_manual, on=['Specialty', 'CPT_Code', 'is_PPL'], how='left')


# In[ ]:


cpts_w_claims1 = pd.merge(cpts_w_claims0, claims, on=['Specialty', 'CPT_Code'], how='left')


# In[ ]:


## For spec/cpt combos that don't have claims data associated, use the average of that cpt across specialties
cpts_w_claims2 = pd.merge(cpts_w_claims1, claims_sum, on='CPT_Code', how='left')


# In[ ]:


cpts_w_claims2['avg_hcp_cost_x'] = np.where(cpts_w_claims2['avg_hcp_cost_x'].isnull(), 
                                             cpts_w_claims2['avg_hcp_cost_y'],
                                             cpts_w_claims2['avg_hcp_cost_x'])


# In[ ]:


cpts_w_claims2.drop(columns=['avg_hcp_cost_y', 'sd_hcp_cost'], inplace=True)


# In[ ]:


cpts_w_claims2.rename(index=str, columns={'avg_hcp_cost_x': 'avg_hcp_cost'}, inplace=True)


# In[ ]:


cpts_w_claims2[cpts_w_claims2['Specialty']=='CARDIOLOGY'].head()


# In[ ]:


cpts_w_claims_fin = cpts_w_claims2


# In[ ]:


cpts_w_claims_fin[(cpts_w_claims_fin['UNITS_man'].isnull())].shape[0]


# ## Calculate ROI

# In[ ]:


cpts_w_claims_fin['sum_cost_denied'] = cpts_w_claims_fin['is_den']*cpts_w_claims_fin['UNITS']*cpts_w_claims_fin['avg_hcp_cost']


# In[ ]:


cpts_w_claims_fin['UNITS_man'] = np.where(cpts_w_claims_fin['UNITS_man'].isnull(), 0, cpts_w_claims_fin['UNITS_man'])


# In[ ]:


cpts_w_claims_fin['ROI'] = cpts_w_claims_fin['sum_cost_denied']/cpts_w_claims_fin['cost_to_review']


# In[ ]:


cpts_w_claims_fin[(cpts_w_claims_fin['ROI'].isnull()) &
                 (cpts_w_claims_fin['Specialty']=='CARDIOLOGY')].head(15)


# In[ ]:


## For groups where we don't know the average cost from 2018, the denominator of ROI is 0, and ROI is undefined. 
## Update the ROI for those to = 100 so they are NOT included in the dictionaries to auto-approve going forward.
cpts_w_claims_fin['ROI'] = np.where(cpts_w_claims_fin['avg_hcp_cost'].isnull(), 100, cpts_w_claims_fin['ROI'])


# In[ ]:


## For groups that were auto-approved at 100%, the denominator of ROI is 0, and ROI is undefined. 
## Update the ROI for those to = 0 so they are included in the dictionaries to auto-approve going forward.
cpts_w_claims_fin['ROI'] = np.where(cpts_w_claims_fin['ROI'].isnull(), 0.01, cpts_w_claims_fin['ROI'])
        


# In[ ]:


cpts_w_claims_fin['fin_aa_rec'] = np.where(cpts_w_claims_fin['ROI']<1, 1, 0)


# In[ ]:


cpts_w_claims_fin[(cpts_w_claims_fin['ROI'].isnull())].shape[0]


# In[ ]:


cpts_w_claims_fin['cost_to_review'].sum()


# ## Create Dictionaries

# In[ ]:


## Model Additional Auto Approvals when different thresholds are set.
## Approach: use "given" threshold to determine which CPT codes are "auto-approve"-able for each specialty
##  - For loop through referrals, return 1 if all CPT codes are on "auto-approve"-able list, else 0 


# In[ ]:


list_o_types_to_pend = ['INPT ADM', 'EMERGENCY ROOM', 'DAY SURG', 'OOA INPT', 
                        'SKILLED NURSING', 'OBSERVATION', 'OB OBSERVATION']


# In[ ]:


cpts_w_claims_fin.reset_index(drop=True, inplace=True)


# In[ ]:


def collect_clinical_decisions(specialty_cpt, list_to_change, spec_list, rsn, new_status=0):
    decision_sources = cpts_w_claims_fin['dec_source'].to_list()
    overrule_reasons = cpts_w_claims_fin['overrule_rsn'].to_list()
    final_decisions = cpts_w_claims_fin['final_decision'].to_list()
    for index, row in specialty_cpt.iterrows():
        if row['Specialty'] in spec_list:
            if row['CPT_Code'] in list_to_change:
                decision_sources[index] = 'clinical'
                overrule_reasons[index] = rsn
                final_decisions[index] = new_status
    specialty_cpt['dec_source'] = decision_sources
    specialty_cpt['overrule_rsn'] = overrule_reasons
    specialty_cpt['final_decision'] = final_decisions
    return specialty_cpt
    


# In[ ]:


## Create new column in cpt_w_claims_fin that will store decision source.
cpts_w_claims_fin['dec_source'] = 'DBA'


# In[ ]:


## Create new column in cpt_w_claims_fin that will store clinical overrule_rsn
cpts_w_claims_fin['overrule_rsn'] = np.nan


# In[ ]:


## Create new column in cpt_w_claims_fin that will store final decision
cpts_w_claims_fin['final_decision'] = cpts_w_claims_fin['fin_aa_rec']


# In[ ]:


spec_dict_EPL = {k: [] for k in list_o_specs}


# In[ ]:


cpts_w_claims_fin[(cpts_w_claims_fin['Specialty']=='DERMATOLOGY') &
                  (cpts_w_claims_fin['is_PPL'] ==1) &
                 (cpts_w_claims_fin['final_decision']==1)].shape[0]


# ## Define codes to ADD or REMOVE from each list

# The general process for creating dictionaries is to do the g&a analysis first.  These are noted with "DBA" (Decision by Analysis.
# 
# Next, in chronological order, we apply clinical decisions. In the first review session (Q2 2019) this was the order:
# 1. A list of PRL codes that Chris McRae said should be auto-approved
# 2. A list of PRL codes that Chris McRae said should be pended - "codestoremove"
# 3. Pend Specialties that should be pended categorically
# 4. Laurie and Chris's first round of mostly pends.  Rhoda also took a pass on 5/3/2019
# 5. Phyllis and Rhoda pass at pended specialties. 5/17/2019
# 6. Remove low volume codes.
# 7. Pend all EPL Codes

# In[ ]:


## ADD Derm codes - provided my Christel McRae (124)
derm_add0 = ['10060','10061','11000','11055','11057','11100','11101','11300','11301','11302','11303','11305',
            '11306','11307','11308','11310','11311','11312','11313','11404','11426','11440','11600',
            '11601','11602','11603','11604','11606','11620','11621','11622','11623','11624','11626','11640',
            '11641','11642','11643','11644','11646','11900','11901','29580','54050','54056','54100','67810',
            '69100','87101','87220','J3301','10061','10080','10081','10140','10180','11055','11056','11057',
            '11101','11300','11301','11302','11303','11305','11306','11307','11308','11310','11311','11312',
            '11313','11404','11426','11440','11444','11446','11600','11601','11602','11603','11604',
            '11606','11620','11621','11622','11623','11624','11626','11640','11641','11642','11643','11644',
            '11646','11900','11901','54105']


# In[ ]:


## find unique values (72)
derm_add = list(set(derm_add0))


# In[ ]:


## ADD Pain Add for PPL
pain_add0 = ['99203','99204','99213','99214']


# In[ ]:


## ADD Gastro and General Surgery for PPL and EPL
gi_add0 = ['45378','45380','45385','G0105','G0121','00812']


# In[ ]:


## ADD Blood Transfusions - across all specialties PPL and EPL
bt_add0 = ['36430','86900','86901','86902','86903','86904','86905','86906','86907','86908','86909','86910'
          ,'86911','86912','86913','86914','86915','86916','86917','86918','86919','86920']


# In[ ]:


## ADD Podiatry
podiatry_add0 = ['11055']


# In[ ]:


## REMOVE from all dictionaries
codes_to_remove0 = ['97810','95115','95116','95117','95170','95180','95181','95182','95183','95184','95185','95186',
                   '95187','95188','95199','95004','A0999','A0426','A0427','A0428','A0429','92590','92591','92592',
                   '92593','92594','90901','90902','90903','90904','90905','90906','90907','90908','90909','90910',
                   '90911','90875','90876','86890','93798','98940','J1050','58301','58565','58611','G0337',
                   '74740','89320','J3490','J0717','J1438','J7321','J7322','J7323','J7324','J7325','J3489','J0135',
                   'J3030','J1830','J9215','J9218','J0129','J9202','J3301','J1745','96365','96366','90378','J2505',
                   'J1440','J1441','J0885','J0886','T1013','96118','96119','96120','97166','97167','97168','97110',
                   '92015','97162','97163','97110','36468','92506','55250','59840',
                   'S0199','S0618','92595','58300','J7300','57170','A4266','11981',
                   'J7302','J7300','58670','59600','58605','64612','J0585','11055',
                   '11057','11719','11721','G0127','S0390']


# In[ ]:


codes_to_remove = list(set(codes_to_remove0))


# In[ ]:


cpts_w_claims_fin[(cpts_w_claims_fin['Specialty']=='GASTROENTEROLOGY') &
                  (cpts_w_claims_fin['is_PPL'] ==1) &
                 (cpts_w_claims_fin['final_decision']==1)].shape[0]


# In[ ]:


cpts_w_claims_fin[(cpts_w_claims_fin['Specialty']=='DERMATOLOGY') &
                  (cpts_w_claims_fin['is_PPL'] ==1) &
                 (cpts_w_claims_fin['final_decision']==1)].shape[0]


# In[ ]:


cpts_w_claims_fin.pivot_table(values='CPT_Code', index=['final_decision'], aggfunc='count', margins=True)


# #### Add specific codes to specific specialties

# In[ ]:


## List of codes to auto-approve add derm
cpts_w_claims_fin = collect_clinical_decisions(cpts_w_claims_fin, derm_add, ['DERMATOLOGY'], rsn='from PRL', new_status=1)


# In[ ]:


## List of codes to auto-approve add pain
cpts_w_claims_fin = collect_clinical_decisions(cpts_w_claims_fin, pain_add0, ['PAIN MANAGEMENT'], rsn='from PRL', new_status=1)


# In[ ]:


## List of codes to auto-approve add gastro
cpts_w_claims_fin = collect_clinical_decisions(cpts_w_claims_fin, gi_add0, 
                                               ['GASTROENTEROLOGY','SURGERY - GENERAL'], rsn='from PRL', new_status=1)


# In[ ]:


## List of codes to auto-approve add blood transfusions
cpts_w_claims_fin = collect_clinical_decisions(cpts_w_claims_fin, bt_add0, list_o_specs, rsn='from PRL', new_status=1)


# In[ ]:


## List of codes to auto-approve add podiatry
cpts_w_claims_fin = collect_clinical_decisions(cpts_w_claims_fin, podiatry_add0, ['PODIATRY'], rsn='from PRL', new_status=1)


# #### Pend specific set of codes across specialties

# In[ ]:


## List of codes to pend across specialties
cpts_w_claims_fin = collect_clinical_decisions(cpts_w_claims_fin, codes_to_remove, list_o_specs, rsn='from PRL', new_status=0)


# #### Soft Pend Specialties categorically

# In[ ]:


## Specialties for which the clinical team prefers to ignore the "decision by analysis" recommendation and instead
## choose a few codes to AA, but mostly the specialty will pend
## To ensure that no referrals are auto-approved from it
specs_that_should_pend = ['ACUPUNCTURE', 'ADDICTION MEDICINE', 'ANESTHESIOLOGY',
                         'BEHAVIORAL HEALTH', 'CHIROPRACTIC', 'DENTIST', 'DME MAINTENANCE',
                         'LICENSED CLIN SOCIAL WORKER', 'MFCC (THERAPIST)', 'NON-CONTRACT UNKWN BILL AREA',
                         'NURSE PRACTITIONER', 'NURSING FACILITY - OTHER', 'OPTICIAN',
                        'PEDS-DEVELOPMENTAL BEHAVIORAL', 'PSYCHIATRY', 'PSYCHOLOGY', 'SENIOR WELLNESS VISIT',
                         'SPORTS MEDICINE', 'SURGERY - ORAL', 'AMBULATORY SURGICAL CENTER', 'AMBULANCE', 'ALLERGY/IMMUNOLOGY',
                         'CUSTODIAL CARE', 'HOME HEALTH', 'INFERTILITY', 'OPTOMETRY', 'PALLIATIVE CARE', 'PODIATRY', 'SLEEP STUDY',
                         'SNF - FAC', 'OCCUPATIONAL THERAPY', 'PHYSICAL THERAPY/REHAB', 'FACILITY SERVICES',
                         'GENETICS', 'INTERVENTIONAL RADIOLOGY', 'LABORATROY', 'NUCLEAR MEDICINE', 'ONCOLOGY - GYN',
                         'PATHOLOGY', 'PHARMACY', 'SURGERY - HAND', 'SURGERY - CARDIAC', 'SURGERY - MAXILLOFACIAL ORAL',
                         'SURGERY - PLASTIC/RECONSTRUCT']


# In[ ]:


for spec in specs_that_should_pend:
    cpts_w_claims_fin['overrule_rsn'] = np.where(cpts_w_claims_fin['Specialty'] == spec, 
                                    'Soft Pend Specialty', cpts_w_claims_fin['overrule_rsn'])
    cpts_w_claims_fin['dec_source'] = np.where(cpts_w_claims_fin['Specialty']==spec, 
                                    'clinical', cpts_w_claims_fin['dec_source'])
    cpts_w_claims_fin['final_decision'] = np.where(cpts_w_claims_fin['Specialty']==spec, 
                                    0, cpts_w_claims_fin['final_decision'])


# In[ ]:


def add_clinical_decisions(cpts_w_claims_fin, clinical_decisions, drop_columns = ['Decision', 'Notes', 'overturned']):
    cpts_w_claims_fin = cpts_w_claims_fin.merge(clinical_decisions, how='left', on=['Specialty', 'CPT_Code', 'is_PPL'])
    cpts_w_claims_fin['Decision'] = np.where(cpts_w_claims_fin['Decision'].isna(), cpts_w_claims_fin['final_decision'], cpts_w_claims_fin['Decision'])
    cpts_w_claims_fin['overrule_rsn'] = np.where(cpts_w_claims_fin['Decision']!=cpts_w_claims_fin['final_decision'], 
                                    cpts_w_claims_fin['Notes'], cpts_w_claims_fin['overrule_rsn'])
    cpts_w_claims_fin['dec_source'] = np.where(cpts_w_claims_fin['Decision']!=cpts_w_claims_fin['final_decision'], 
                                    'clinical', cpts_w_claims_fin['dec_source'])
    cpts_w_claims_fin['final_decision'] = np.where(cpts_w_claims_fin['Decision']!=cpts_w_claims_fin['final_decision'], 
                                    cpts_w_claims_fin['Decision'], cpts_w_claims_fin['final_decision'])
    cpts_w_claims_fin.drop(drop_columns, axis=1, inplace=True)
    return cpts_w_claims_fin


# #### Remove codes chosen by Laurie and Christel 5/03/2019

# In[ ]:


cpts_w_claims_fin = add_clinical_decisions(cpts_w_claims_fin, clinical_decisions_20190503_remove)


# #### Add codes chosen by Laurie and Christel 5/03/2019

# In[ ]:


cpts_w_claims_fin = add_clinical_decisions(cpts_w_claims_fin, clinical_decisions_20190503_add)


# #### Add codes chosen by Phyllis and Rhoda 5/17/2019

# In[ ]:


cpts_w_claims_fin = add_clinical_decisions(cpts_w_claims_fin, clinical_decisions_20190517)


# #### codes chosen by Laurie and Rhoda 5/22/2019

# In[ ]:


cpts_w_claims_fin = add_clinical_decisions(cpts_w_claims_fin, clinical_decisions_20190522, ['Decision', 'Notes'])


# #### Pend codes with fewer than 30 in the past year

# In[ ]:


## Pend codes that have volume < 30
cpts_w_claims_fin['overrule_rsn'] = np.where(cpts_w_claims_fin['UNITS']<=30, 
                                    'low volume', cpts_w_claims_fin['overrule_rsn'])


# In[ ]:


cpts_w_claims_fin['dec_source'] = np.where(cpts_w_claims_fin['UNITS']<=30, 
                                    'Rule', cpts_w_claims_fin['dec_source'])


# In[ ]:


cpts_w_claims_fin['final_decision'] = np.where(cpts_w_claims_fin['UNITS']<=30, 
                                    0, cpts_w_claims_fin['final_decision'])


# #### Hard Pend Specialties categorically

# In[ ]:


## Specialties that the clinical team wants to pend categorically

specs_that_should_pend = ['ACUPUNCTURE', 'ADDICTION MEDICINE', 'ANESTHESIOLOGY',
                         'BEHAVIORAL HEALTH', 'CHIROPRACTIC', 'DENTIST', 'DME MAINTENANCE',
                         'LICENSED CLIN SOCIAL WORKER', 'MFCC (THERAPIST)', 'NON-CONTRACT UNKWN BILL AREA',
                         'NURSE PRACTITIONER', 'NURSING FACILITY - OTHER', 'OPTICIAN',
                        'PEDS-DEVELOPMENTAL BEHAVIORAL', 'PSYCHIATRY', 'PSYCHOLOGY', 'SENIOR WELLNESS VISIT',
                         'SPORTS MEDICINE', 'SURGERY - ORAL', 'AMBULATORY SURGICAL CENTER', 'AMBULANCE',
                         'CUSTODIAL CARE', 'HOME HEALTH', 'INFERTILITY', 'OPTOMETRY', 'PALLIATIVE CARE', 'SLEEP STUDY',
                         'SNF - FAC', 'OCCUPATIONAL THERAPY', 'FACILITY SERVICES', 'NUCLEAR MEDICINE',
                         'PATHOLOGY', 'PHARMACY', 'SURGERY - HAND']


# In[ ]:


for spec in specs_that_should_pend:
    cpts_w_claims_fin['overrule_rsn'] = np.where(cpts_w_claims_fin['Specialty'] == spec, 
                                    'Hard Pend Specialty', cpts_w_claims_fin['overrule_rsn'])
    cpts_w_claims_fin['dec_source'] = np.where(cpts_w_claims_fin['Specialty']==spec, 
                                    'clinical', cpts_w_claims_fin['dec_source'])
    cpts_w_claims_fin['final_decision'] = np.where(cpts_w_claims_fin['Specialty']==spec, 
                                    0, cpts_w_claims_fin['final_decision'])


# #### Pend all EPL

# In[ ]:


## Pend EPL
cpts_w_claims_fin['overrule_rsn'] = np.where(cpts_w_claims_fin['is_PPL']==0, 
                                    'Pend EPL', cpts_w_claims_fin['overrule_rsn'])


# In[ ]:


cpts_w_claims_fin['dec_source'] = np.where(cpts_w_claims_fin['is_PPL']==0, 
                                    'clinical', cpts_w_claims_fin['dec_source'])


# In[ ]:


cpts_w_claims_fin['final_decision'] = np.where(cpts_w_claims_fin['is_PPL']==0, 
                                    0, cpts_w_claims_fin['final_decision'])


# In[ ]:


## Function that gathers CPT codes that should be pended and stores them in a dictionary.
def create_dict_of_CPT_codes_v3(specialty_cpt, list_o_specs, PPL=1):
    spec_dict = {k: [] for k in list_o_specs}
    for index, row in specialty_cpt.iterrows():
        if row['final_decision'] == 1:
            if row['is_PPL'] == PPL:
                spec_dict[row['Specialty']].append(row['CPT_Code'])
    return spec_dict


# In[ ]:


spec_dict_PPL = create_dict_of_CPT_codes_v3(cpts_w_claims_fin, list_o_specs, PPL=1)


# In[ ]:


#### Prepare a newer set of referrals to run through the new code.


# In[ ]:


## Assigns an auto-approve (1) or pend (0) status to each specialty / code group
def assign_status(codes, spec_dict_PPL, spec_dict_EPL, list_of_types_pend):
    status = list(np.zeros(codes.shape[0]))
    specs_list = codes['Specialty'].unique().tolist()
    print(len(status))
    print(codes.shape[0])
    for spec in specs_list:
        if spec not in spec_dict_PPL:
            spec_dict_PPL[spec] = []
    for spec in specs_list:
        if spec not in spec_dict_EPL:
            spec_dict_EPL[spec] = []
    for index, row in codes.iterrows():
        if row['is_PPL'] == 1:
            if row['CPT_Code'] in spec_dict_PPL[row['Specialty']]:
                if row['ref_type'] not in list_of_types_pend:
                    status[index] = 1
        ##else:
          ##  if row['CPT_Code'] in spec_dict_EPL[row['Specialty']]:
            ##    status[index] = 0
    return status


# In[ ]:


cpts_new.reset_index(drop=True, inplace=True)


# In[ ]:


auto_approve = assign_status(cpts_new, spec_dict_PPL, 
                             spec_dict_EPL, list_o_types_to_pend)


# In[ ]:


cpts_new['auto_approvable'] = auto_approve


# In[ ]:


refs_results = cpts_new.groupby(['HCP_CONNECT_AUTH_NUMBER'], as_index=False).agg({'auto_approvable': 'mean',
                                                                                 'is_den': 'mean'})


# In[ ]:


refs_results['aa-yn'] = np.where(refs_results['auto_approvable']==1, 1, 0)


# In[ ]:


refs_results.drop('auto_approvable', axis=1, inplace=True)


# In[ ]:


refs_results.pivot_table('HCP_CONNECT_AUTH_NUMBER', index='aa-yn', columns='is_den', aggfunc='count', margins=True)


# In[ ]:


refs_results.drop('is_den', axis=1, inplace=True)


# In[ ]:


cpts_new = cpts_new.merge(refs_results, how='left', on=['HCP_CONNECT_AUTH_NUMBER'])


# In[ ]:


cpts_w_claims_fin[cpts_w_claims_fin['CPT_Code']=='J1561'].head()


# In[ ]:


cpts_new.pivot_table(values='UNITS', index=['is_autoapp', 'is_den'], columns='aa-yn', aggfunc='count', margins=True)##.to_csv('../Data/Outputs/costs_cm.csv')


# ## Construct Specialty Summary

# In[ ]:


refs_new_head = cpts_new.groupby(['Specialty', 'ref_type', 'region', 'HCP_CONNECT_AUTH_NUMBER'], as_index=False).agg({
    'UNITS': 'count',
    'is_autoapp': 'mean',
    'is_PPL': 'mean',
    'is_app': 'mean',
    'is_den': 'mean',
    'aa-yn': 'mean'
})


# In[ ]:


refs_new_head.pivot_table(values=['is_autoapp', 'aa-yn'], index='Specialty', aggfunc=['count', 'mean']).to_csv('../Data/Outputs/spec_rates.csv',
                                                                                                              sep='|')


# In[ ]:


specialty_summary = pd.read_csv('../Data/Outputs/spec_rates.csv', sep='|', skiprows=3, names=['Specialty',
                                                                                             'ref_vol', 'ref_vol2',
                                                                                             'new_rate', 'old_rate'])


# In[ ]:


specialty_summary.drop('ref_vol2', axis=1, inplace=True)


# In[ ]:


## Get counts of codes for each specialty


# In[ ]:


def spec_code_counts(spec_dict, spec_summary):
    counts = []
    for index, row in spec_summary.iterrows():
        counts.append(len(spec_dict[row['Specialty']]))
    return counts


# In[ ]:


PPL_counts = spec_code_counts(spec_dict_PPL, specialty_summary)


# In[ ]:


specialty_summary['num_codes']=PPL_counts


# In[ ]:


## get 'denied now approved'


# In[ ]:


refs_new_head['denied_now_aa'] = refs_new_head['is_den'] * refs_new_head['aa-yn']


# In[ ]:


refs_new_head.pivot_table('denied_now_aa', index='Specialty', aggfunc='sum').to_csv('../Data/Outputs/spec_den_now_aa.csv',
                                                                                                      sep='|')


# In[ ]:


ss_deny_now_aa = pd.read_csv('../Data/Outputs/spec_den_now_aa.csv', sep='|', skiprows=2, names=['Specialty', 'deny_now_aa'])


# In[ ]:


specialty_summary = specialty_summary.merge(ss_deny_now_aa, how='inner', on='Specialty')


# In[ ]:


## Prep cpts_w_claims_fin to merge into cpts_new


# In[ ]:


cpts_w_claims_fin_select = cpts_w_claims_fin.drop(['UNITS', 'is_autoapp', 'is_den',
       'UNITS_man', 'cost_to_review', 'cnt_hcp_cost',
       'sum_cost_denied'], axis=1)


# In[ ]:


cpts_new_w_fin = pd.merge(cpts_new, cpts_w_claims_fin_select, how='left', on=['Specialty', 'CPT_Code', 'is_PPL'])


# In[ ]:


cpts_new_w_fin['dollars_denied_now_app'] = cpts_new_w_fin['is_den']*cpts_new_w_fin['aa-yn']*cpts_new_w_fin['avg_hcp_cost']


# In[ ]:


cpts_new_w_fin['cost_to_review_bene'] = (1-cpts_new_w_fin['is_autoapp'])*cpts_new_w_fin['aa-yn']*ga_cpt


# In[ ]:


cpts_new_w_fin['cost_to_review_loss'] = cpts_new_w_fin['is_autoapp']*(1-cpts_new_w_fin['aa-yn'])*ga_cpt


# In[ ]:


cpts_new_w_fin['new_denial_bene_est'] = cpts_new_w_fin['is_autoapp']*(1-cpts_new_w_fin['aa-yn'])*ga_cpt*cpts_new_w_fin['ROI']*cpts_new_w_fin['is_PPL']


# In[ ]:


cpts_new_w_fin.pivot_table(values=['dollars_denied_now_app', 'cost_to_review_bene', 'cost_to_review_loss',
                                  'new_denial_bene_est'], index='Specialty', aggfunc='sum').to_csv('../Data/Outputs/specialty_fins.csv', sep='|')


# In[ ]:


specialty_fins = pd.read_csv('../Data/Outputs/specialty_fins.csv', sep='|')


# In[ ]:


specialty_summary = specialty_summary.merge(specialty_fins, how='inner', on='Specialty')


# In[ ]:


specialty_summary['net_benefit'] = specialty_summary['cost_to_review_bene'] - specialty_summary['cost_to_review_loss'] - specialty_summary['dollars_denied_now_app']


# In[ ]:


specialty_summary['net_benefit_w_new_den'] = specialty_summary['cost_to_review_bene'] - specialty_summary['cost_to_review_loss'] -specialty_summary['dollars_denied_now_app'] + specialty_summary['new_denial_bene_est']


# In[ ]:


# Give the filename you wish to save the file to
spec_summary_filename = '../Data/Outputs/specialty_summary_fin.xlsx'

# Use this function to search for any files which match your filename
files_present = os.path.isfile(spec_summary_filename)

# if no matching files, write to csv, if there are matching files, print statement
if not files_present:
    specialty_summary.to_excel(spec_summary_filename, index=False, float_format='%.2f',
                          header=['Specialty', 'volume', 'new rate', 'old_rate', '# codes in dictionary', 'false positives',
                                 'cost to review, benefit', 'cost to review, loss', 'dollars denied, now approved',
                                 'new denials, benefit (est)', 'new benefit', 'net benefit, est'], 
                          freeze_panes=(1,0))
else:
    print('WARNING: This file already exists!')


# In[ ]:


cpts_w_claims_fin['overrule_rsn'].unique()


# In[ ]:


refs_results['aa-yn'].mean()


# In[ ]:


## rate before laurie and chrisel's codes to pend
v0 = 1198038/2086468
print(v0)


# In[ ]:


## rate after laurie and christels' codes to pend
v1 = 1091109/2086468
print(v1)


# In[ ]:


v2 = 1097619/2086468
print(v2)


# In[ ]:


## using 2018 data to create rules, and then applying them to early 2019 data
v3 = 274250/543258
print(v3)


# In[ ]:


## using clinical review list from 20190501 from Rhoda 
.401


# In[ ]:


## 20190503 V1 - More Conservative Version
.4532


# In[ ]:


## 20190503 v2 - Baseline
.462


# In[ ]:


## 20190517 - after Phyllis and Rhoda add back several codes
.52


# In[ ]:


## 20190524
.525


# ## Code Detail List

# In[ ]:


spec_cpt_w_projected_aa_rate = cpts_new.groupby(['Specialty', 'CPT_Code', 'is_PPL'], as_index=False).agg({
    'aa-yn': 'mean'
})


# In[ ]:


cpts_w_claims_fin = cpts_w_claims_fin.merge(spec_cpt_w_projected_aa_rate, how='left', on=['Specialty', 'CPT_Code', 'is_PPL'])


# In[ ]:


cpts_new['UNITS'].sum()


# In[ ]:


# Give the filename you wish to save the file to
spec_cpt_w_projected_aa_rate_fin_filename = '../Data/Outputs/spec_cpt_w_projected_aa_rate.xlsx'

# Use this function to search for any files which match your filename
files_present = os.path.isfile(spec_cpt_w_projected_aa_rate_fin_filename)

# if no matching files, write to csv, if there are matching files, print statement
if not files_present:
    spec_cpt_w_projected_aa_rate.to_excel(spec_cpt_w_projected_aa_rate_fin_filename, index=False, float_format='%.2f',
                          header=['Specialty', 'CPT Code', 'PPL?', 'new rate'], 
                          freeze_panes=(1,0))
else:
    print('WARNING: This file already exists!')


# In[ ]:


cpts_new.pivot_table('CPT_Code', index='aa-yn', columns='is_den', aggfunc='count', margins=True)


# ## Spot Check Specific Referrals

# In[ ]:


cpts_new[(cpts_new['Specialty']=='OCCUPATIONAL THERAPY') 
             ##& (refs_results['status_name']=='DENIED - CM')
            & (cpts_new['aa-yn']==1)].tail(10)


# ## Referral Detail Lookup (Enter Auth)

# In[ ]:


cpts_new[(cpts_new['is_den']==1) &
        (cpts_new['aa-yn']==1)].to_csv('../Data/Outputs/false_positives_from_new.csv')


# ## Auth Lookup (Enter Specialty and Code)

# In[ ]:


cpts_new[(cpts_new['Specialty']=='ENT-OTOLARYNGOLOGY')
        & (cpts_new['CPT_Code']=='95024')
        ##& (refs_det['auto_approvable'] == 0)
        ]


# In[ ]:


cpts_new[(cpts_new['Specialty']=='ENT-OTOLARYNGOLOGY')
        & (cpts_new['HCP_CONNECT_AUTH_NUMBER']=='13418107H')
        ##& (refs_det['auto_approvable'] == 0)
        ]


# In[ ]:


cpts_w_claims_fin.drop(['cnt_hcp_cost'], axis=1, inplace=True)


# In[ ]:


cpts_w_claims_fin = cpts_w_claims_fin.merge(cpt_desc, how='inner', on = 'CPT_Code')


# In[ ]:


cpts_w_claims_fin[cpts_w_claims_fin['Specialty']=='CARDIOLOGY']


# In[ ]:


# Give the filename you wish to save the file to
cpts_w_claims_fin_filename = '../Data/Outputs/spec_cpt_summary.xlsx'

# Use this function to search for any files which match your filename
files_present = os.path.isfile(cpts_w_claims_fin_filename)

# if no matching files, write to csv, if there are matching files, print statement
if not files_present:
    cpts_w_claims_fin.to_excel(cpts_w_claims_fin_filename, index=False, float_format='%.2f',
                          header=['Specialty', 'CPT Code', 'PPL?', 'volume', 'old rate', 'denial rate',
       'volume, reviewed', 'cost to review', 'avg. cost', 'denied dollars', 'ROI',
       'decision by ROI', 'decision source', 'clinical reason', 'final decision', 'new rate',
       'CPT desc'], 
                          freeze_panes=(1,0))
else:
    print('WARNING: This file already exists!')


# In[ ]:





# In[ ]:




