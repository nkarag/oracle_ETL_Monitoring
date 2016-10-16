/*
διόρθωσε
    1. ETL_MONIOTR.sql CTO-check query  --> OK
    2. cto_wait.sql script  --> OK
    3. DWADMIN.CONC_STATS_COLLECTION.CTO_HAS_FINISHED --> OK
    4. Απο script ΓΙώργου
        α. Τσίμπησε όλους τους κόμβους που δείχνουν το πότε τελείωσε μια ροή
        β. Σημείωσε τα ως milestones στα comments στο query που δείχνει χρόνους ανα Node και πόσο θέλει για το milestone
        γ. ενημέρωσε script που δείχνει πότε τελειώνουν οι main flows (?) ή φτιάξε καινούριο (επηρρεάζει και το owb_pa.sql)
        d. φτιάξε νέο query που να δείχνει για CTO & NMR ποιός το χρονισμό όλων των προαπαιτούμενων ροών (βάσει του script του Γιώργου)
    
*/

 SELECT RUN_DATE        
        FROM STAGE_DW.DW_CONTROL_TABLE        
        WHERE PROCEDURE_NAME = 'CTO_LAST_RUN'
-- δείχνει την ημ/νια που έχει φορτώσει


select *
from dba_procedures
  where
    procedure_name like 'CHECK%CTO%'
    
    
    ETL_SBL_TRANSFORMATION_PKG.CHECK_FLOWSEND_FORCTO_PROC --> checks if CTO is running. IF not it updates the 'CTO_END_DATE to a future date, to denote execution of the CTO
    
    ETL_SBL_TRANSFORMATION_PKG.CHECK_FLOW_FORCTOWFM_PROC
    
           -- DAILY DWH    
          SELECT RUN_DATE + 1 INTO temp_date_dw
            FROM STAGE_DW.DW_CONTROL_TABLE      
            WHERE PROCEDURE_NAME = 'OTE_DW_LAST_RUN';     
        -- GENESYS
          SELECT RUN_DATE + 1  
            INTO temp_genesys_date    
            FROM STAGE_DW.DW_CONTROL_TABLE
            WHERE PROCEDURE_NAME = 'GENESYS_LAST_RUN';     
        -- WFM
          SELECT RUN_DATE + 1  
            INTO temp_wfm_date
            FROM STAGE_DW.DW_CONTROL_TABLE
            WHERE PROCEDURE_NAME = 'WFM_LAST_RUN';     
        -- CTO KPI    
          SELECT RUN_DATE + 1 INTO temp_date_cto
            FROM STAGE_DW.DW_CONTROL_TABLE
            WHERE PROCEDURE_NAME = 'CTO_LAST_RUN';     
        --   
          WHILE temp_date_cto >= temp_date_dw OR 
                temp_date_cto >= temp_wfm_date OR
                temp_date_cto >= temp_genesys_date LOOP     
    
    
    ETL_SBL_TRANSFORMATION_PKG.CHECK_FLOW_FORCTOORDER_PROC
    
    -- ΠΕΡΙΦΕΡΕΙΕΣ    
  SELECT FLOW_BASEDATE
    INTO temp_date    
    FROM STAGE_PERIF.FLOW_PROGRESS_STG      
    WHERE FLOW_NAME = 'PERIF_PRESENT_AREA_END';      
-- DAILY DWH    
  SELECT RUN_DATE + 1 INTO temp_date_dw
    FROM STAGE_DW.DW_CONTROL_TABLE      
    WHERE PROCEDURE_NAME = 'OTE_DW_LAST_RUN';     
-- FAULTS
  SELECT RUN_DATE + 1  
    INTO temp_crm_date    
    FROM STAGE_DW.DW_CONTROL_TABLE
    WHERE PROCEDURE_NAME = 'FAULT_LAST_RUN';     
-- SIEBEL    
  SELECT RUN_DATE + 1 
    INTO temp_siebel_date      
    FROM STAGE_DW.DW_CONTROL_TABLE        
    WHERE PROCEDURE_NAME = 'SOC_END_FORNMR_DATE';       
-- CTO KPI    
  SELECT RUN_DATE + 1 INTO temp_date_cto
    FROM STAGE_DW.DW_CONTROL_TABLE
    WHERE PROCEDURE_NAME = 'CTO_LAST_RUN';     
--   
  WHILE temp_date_cto >= temp_date OR 
        temp_date_cto >= temp_date_dw OR 
        temp_date_cto >= temp_siebel_date  OR
        temp_date_cto >= temp_crm_date LOOP    
                
    
    
    ETL_SBL_TRANSFORMATION_PKG.CHECK_FLOW_FORCTOFAULT_PROC
    
        -- ΠΕΡΙΦΕΡΕΙΕΣ    
      SELECT FLOW_BASEDATE
        INTO temp_date    
        FROM STAGE_PERIF.FLOW_PROGRESS_STG      
        WHERE FLOW_NAME = 'PERIF_PRESENT_AREA_END';      
    -- DAILY DWH    
      SELECT RUN_DATE + 1 INTO temp_date_dw
        FROM STAGE_DW.DW_CONTROL_TABLE      
        WHERE PROCEDURE_NAME = 'OTE_DW_LAST_RUN';     
    -- FAULTS
      SELECT RUN_DATE + 1  
        INTO temp_crm_date    
        FROM STAGE_DW.DW_CONTROL_TABLE
        WHERE PROCEDURE_NAME = 'FAULT_LAST_RUN';     
    -- CTO KPI    
      SELECT RUN_DATE + 1 INTO temp_date_cto
        FROM STAGE_DW.DW_CONTROL_TABLE
        WHERE PROCEDURE_NAME = 'CTO_LAST_RUN';     
    --   
      WHILE temp_date_cto >= temp_date OR 
                temp_date_cto >= temp_date_dw OR 
                temp_date_cto >= temp_crm_date  LOOP  
                
-- ***********  1. ETL_MONIOTR.sql CTO-check query

with
level0 as
(
    -- DAILY DWH    
    SELECT RUN_DATE + 1 temp_level0_dw
    FROM STAGE_DW.DW_CONTROL_TABLE      
    WHERE PROCEDURE_NAME = 'OTE_DW_LAST_RUN' 
),
genesis as
(
    -- GENESYS
    SELECT RUN_DATE + 1 temp_genesys_date    
    FROM STAGE_DW.DW_CONTROL_TABLE
    WHERE PROCEDURE_NAME = 'GENESYS_LAST_RUN'
),
wfm as
(                 
    -- WFM
    SELECT RUN_DATE + 1 temp_wfm_date
    FROM STAGE_DW.DW_CONTROL_TABLE
    WHERE PROCEDURE_NAME = 'WFM_LAST_RUN'
),
per_main as
(
    -- ΠΕΡΙΦΕΡΕΙΕΣ    
    SELECT FLOW_BASEDATE temp_per_date    
    FROM STAGE_PERIF.FLOW_PROGRESS_STG      
    WHERE FLOW_NAME = 'PERIF_PRESENT_AREA_END'      
),
faults as
(
    -- FAULTS
    SELECT RUN_DATE + 1 temp_faults_date    
    FROM STAGE_DW.DW_CONTROL_TABLE
    WHERE PROCEDURE_NAME = 'FAULT_LAST_RUN'
),         
soc4nmr as
(
    -- SIEBEL    
    SELECT RUN_DATE + 1 temp_soc4nmr_date      
    FROM STAGE_DW.DW_CONTROL_TABLE        
    WHERE PROCEDURE_NAME = 'SOC_END_FORNMR_DATE'
),       
cto as
(                 
    -- CTO KPI    
    SELECT RUN_DATE + 1 temp_date_cto
    FROM STAGE_DW.DW_CONTROL_TABLE
    WHERE PROCEDURE_NAME = 'CTO_LAST_RUN'
)         
select
    temp_date_cto CTO_TO_BE_LOADED,
    -- WFM subflow: WFM KPIs και Genesis
    temp_level0_dw,
    temp_wfm_date,
    temp_genesys_date,
    CASE WHEN 
            temp_date_cto >= temp_level0_dw OR 
            temp_date_cto >= temp_wfm_date OR
            temp_date_cto >= temp_genesys_date
        THEN    'WAITING'
        ELSE    'OK'        
    END  WFM_SUBFLOW_STATUS,              
    -- FAULTS subflow: FAULTS KPIs Siebel Faults και Προμηθέα (LL, καλωδιακές)
    temp_per_date,
    temp_level0_dw,
    temp_faults_date,
    CASE WHEN 
            temp_date_cto >= temp_per_date OR 
            temp_date_cto >= temp_level0_dw OR 
            temp_date_cto >= temp_faults_date         
        THEN    'WAITING'
        ELSE    'OK'        
    END  FAULTS_SUBFLOW_STATUS,                     
    -- ORDERS subflow: ORDER KPIs από Siebel, Woms, Προμηθέα
    temp_per_date,
    temp_level0_dw,
    temp_soc4nmr_date,
    temp_faults_date,
    CASE WHEN 
            temp_date_cto >= temp_per_date OR 
            temp_date_cto >= temp_level0_dw OR 
            temp_date_cto >= temp_soc4nmr_date  OR
            temp_date_cto >= temp_faults_date         
        THEN    'WAITING'
        ELSE    'OK'        
    END  ORDERS_SUBFLOW_STATUS
from level0, genesis, wfm, per_main, faults, soc4nmr, cto;                     
    
        