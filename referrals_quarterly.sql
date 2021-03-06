SELECT rd.[PX_CODE] as CPT_Code
	    ,rh.[HCP_CONNECT_AUTH_NUMBER]
      ,rh.[REF_TYPE_KEY]
      ,rh.[PPL]
      ,rh.[DT_RECEIVED] as Date_Received
      ,rh.[DECISION_DT] as Date_Decision
      ,lrt.[REFERRAL_TYPE_NAME] as ref_type
      ,lrs.[STATUS_TYPE] as status_cat
      ,lrs.[NAME] as status_name
      ,lus.[BILLING_AREA] as Specialty
      ,ls.[BUDGET_COMMUNITY] as region
      ,lup.[NAME] as urgent
      ,rd.[UNITS]
      ,lbp.[PRODUCT_CATEGORY_NAME] as LOB
  FROM [IADS_V3].[dbo].[REFERRAL_DET] rd
    inner join [IADS_V3].[dbo].[REFERRAL_HDR] rh on rd.[REFERRAL_KEY] = rh.[REFERRAL_KEY]
    inner join [IADS_V3].[dbo].[LU_REFERRING_PROVIDER] lrf on rh.[REFERRING_PROV_KEY] = lrf.[Record_Number]
    inner join [IADS_V3].[dbo].[LU_Specialty] lus on rh.[SPECIALTY_REF_TO_PROVIDER_KEY] = lus.[Record_Number]
    inner join [IADS_V3].[dbo].[LU_ELIGIBILITY] le on rh.[ELIG_KEY] = le.[ELIG_KEY]
    inner join [IADS_V3].[dbo].[LU_PRIORITY] lup on rh.[PRIORITY_KEY] = lup.[Record_Number]
    inner join [IADS_V3].[dbo].[LU_SITE] ls on le.[HCP_SITE_KEY] = ls.[Record_Number]
    inner join [IADS_V3].[dbo].[LU_REFERRAL_STATUS] lrs on rh.[REFERRAL_STATUS_KEY] = lrs.[Record_Number]
    inner join [IADS_V3].[dbo].[LU_REFERRAL_TYPE] lrt on rh.[REF_TYPE_KEY] = lrt.[REF_TYPE]
    inner join [IADS_V3].[dbo].[LU_PX_CPT] lucpt on lucpt.[PROCEDURE_CODE] = rd.[PX_CODE]
    inner join [IADS_V3].[dbo].[LU_BENEFIT_PLAN] lbp on lbp.[PATIENT_PLAN_KEY] = rh.[PATIENT_PLAN_KEY]
  WHERE
    rh.[DT_RECEIVED] BETWEEN '2019-05-02' and '2019-09-28' 
  AND ls.[BUDGET_COMMUNITY] in ('San Gabriel Valley', 'South Bay', 'San Fernando Valley', 'Orange County', 'LA/Downtown', 'Long Beach', 'Magan')