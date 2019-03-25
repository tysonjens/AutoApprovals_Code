/****** Script for SelectTopNRows command from SSMS  ******/
SELECT --fc.[Enc_Key]
      --,fc.[Claim_SK]
      --,dpat.[EMPI]
      --,dpat.[Sex]
      --,fc.[Claim_Number]
      --,dd.[DATE_BK]
      --,fc.[Product_SK]
      --,fc.[Healthplan_SK]
      --,fc.[Site_Market_Hierarchy_SK]
      --,bsmh.[Market_SK]
      --,MIN(dm.[Region_Name])
      --,dm.[Super_Region_Name]
      --,fc.[PCP_SK]
      --,dpos.[Place_of_Service_Name]
      --,MIN(dv.[Vendor_Name])
      --,dv.[Vendor_TIN]
      --,fc.[Rendering_Provider_SK]
      ds.[Specialty_Long_Name] as Specialty
      --,fc.[ICD_PX_Group_SK]
      --,fc.[Original_FSC_Key]
      ,dpc.[Procedure_Code] as CPT_Code
      --,fc.[Drug_SK]
      --,fc.[Capitation_Flag]
      --,fc.[APP_Amount]
      --,fc.[APP_Unit]
      --,fc.[Billed_Unit]
      --,fc.[Billed_Amount]
    ,COUNT(fc.[HCP_Cost]) as cnt_hcp_cost
      ,AVG(fc.[HCP_Cost]) as avg_hcp_cost
    ,STDEV(fc.[HCP_Cost]) as sd_hcp_cost
    --,dft.[Fund_Type_Name]
  FROM [National_Analytics].[dbo].[FACT_CLAIM] fc
    inner join [National_Analytics].[dbo].[DIM_VENDOR] dv on fc.[Vendor_SK] = dv.[Vendor_SK]
    inner join [National_Analytics].[dbo].[DIM_DATE] dd on fc.[Date_Service_From_SK] = dd.[DATE_SK]
    inner join [National_Analytics].[dbo].[DIM_PATIENT] dpat on fc.[Patient_SK] = dpat.[Patient_SK]
    inner join [National_Analytics].[dbo].[BRIDGE_SITE_MARKET_HIERARCHY] bsmh on fc.[Site_Market_Hierarchy_SK] = bsmh.[Site_Market_Hierarchy_SK]
    inner join [National_Analytics].[dbo].[DIM_MARKET] dm on bsmh.[Market_SK] = dm.[Market_SK]
    inner join [National_Analytics].[dbo].[DIM_PLACE_OF_SERVICE] dpos on fc.[Place_of_Service_SK] = dpos.[Place_of_Service_SK]
    inner join [National_Analytics].[dbo].[DIM_SPECIALTY] ds on fc.[Specialty_SK] = ds.[Specialty_SK]
    inner join [National_Analytics].[dbo].[DIM_PROCEDURE_CODE] dpc on fc.[Procedure_Code_SK] = dpc.[Procedure_Code_SK]
  inner join [NATIONAL_ANALYTICS].[dbo].[DIM_FUND_TYPE] dft on fc.[Fund_Type_SK] = dft.[Fund_Type_SK]
  WHERE
    dd.[DATE_BK] BETWEEN '2018-08-01' AND '2018-11-01'
    AND dft.Fund_Type_Name = 'PROFESSIONAL'
    AND dm.[Super_Region_Name] = 'SOUTHERN CALIFORNIA'
    AND fc.[Capitation_Flag] = 0
  --AND ds.[Specialty_Long_Name] = 'CARDIOLOGY'
  --AND dv.[Vendor_Name] = 'DALAL MONA'
  AND fc.HCP_Cost > 0
  GROUP BY
  ds.Specialty_Long_Name,
  dpc.Procedure_Code
  ORDER BY
  ds.Specialty_Long_Name,
  dpc.Procedure_Code