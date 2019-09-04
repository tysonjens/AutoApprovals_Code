SELECT ds.[Specialty_Long_Name] as Specialty
      ,dpc.[Procedure_Code] as CPT_Code
      ,COUNT(fc.[HCP_Cost]) as cnt_hcp_cost
      ,AVG(fc.[HCP_Cost]) as avg_hcp_cost
      ,STDEV(fc.[HCP_Cost]) as sd_hcp_cost
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
    dd.[DATE_BK] BETWEEN '2019-01-01' AND '2019-07-30'
    AND dft.Fund_Type_Name = 'PROFESSIONAL'
    AND dm.[Super_Region_Name] = 'SOUTHERN CALIFORNIA'
    AND fc.[Capitation_Flag] = 0
    AND fc.HCP_Cost > 0
GROUP BY ds.Specialty_Long_Name, dpc.Procedure_Code
ORDER BY ds.Specialty_Long_Name, dpc.Procedure_Code