/****** Script for SelectTopNRows command from SSMS  ******/
SELECT rd.[PX_CODE] as CPT_Code
      --,rh.[REF_TYPE_KEY]
      ,rh.[PPL]
      --,rh.[REFERRING_PROV_KEY]
      ,rh.[DT_RECEIVED] as Date_Received
    ,rh.[DECISION_DT] as Date_Decision
      ,lrt.[REFERRAL_TYPE_NAME] as Type
      ,lrs.[STATUS_TYPE] as status_cat
    ,lrs.[NAME] as status_name
      --,rtp.[NUMERIC_CODE]
      --,rtp.[NAME] as reftoprov
      --,lrf.[NAME] as refer_prov
      ,lus.[BILLING_AREA] as Specialty
      --,ls.[BUDGET_SITE] as Site
      ,ls.[BUDGET_COMMUNITY] as region
    ,rd.[UNITS]
  
  FROM [IADS_V3].[dbo].[REFERRAL_DET] rd
    inner join [IADS_V3].[dbo].[REFERRAL_HDR] rh on rd.[REFERRAL_KEY] = rh.[REFERRAL_KEY]
    inner join [IADS_V3].[dbo].[LU_REFERRING_PROVIDER] lrf on rh.[REFERRING_PROV_KEY] = lrf.[Record_Number]
    --inner join [IADS_V3].[dbo].[LU_REFERRING_TO_PROVIDER] rtp on rh.[REFERRING_TO_PROVIDER_KEY] = rtp.[Record_Number]
    inner join [IADS_V3].[dbo].[LU_Specialty] lus on rh.[SPECIALTY_REF_TO_PROVIDER_KEY] = lus.[Record_Number]
    inner join [IADS_V3].[dbo].[LU_ELIGIBILITY] le on rh.[ELIG_KEY] = le.[ELIG_KEY]
    inner join [IADS_V3].[dbo].[LU_SITE] ls on le.[HCP_SITE_KEY] = ls.[Record_Number]
    inner join [IADS_V3].[dbo].[LU_REFERRAL_STATUS] lrs on rh.[REFERRAL_STATUS_KEY] = lrs.[Record_Number]
    inner join [IADS_V3].[dbo].[LU_REFERRAL_TYPE] lrt on rh.[REF_TYPE_KEY] = lrt.[REF_TYPE]
  WHERE
    rh.[DT_RECEIVED] BETWEEN '2018-08-01' and '2018-11-01' 
    --AND lrs.[STATUS_TYPE] IN ('APPROVED') 
    --AND lrt.[REFERRAL_TYPE_NAME] = 'PHYSICIAN'
  --AND rd.[PX_CODE] in ('99201', '99202', '99203', '99204', '99205', '99211', '99212', '99213', '99214', '99215')
  AND ls.[BUDGET_COMMUNITY] in ('San Gabriel Valley', 'South Bay', 'San Fernando Valley', 'Orange County', 'LA/Downtown', 'Long Beach', 'Magan')

