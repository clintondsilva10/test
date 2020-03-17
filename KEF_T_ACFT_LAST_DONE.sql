SELECT
   ROW_NUMBER() OVER (PARTITION BY ACFT_TASKS.part_no_oem, ACFT_TASKS.serial_no_oem, ACFT_TASKS.task_cd ORDER BY sys_guid()) as ROW_NUM,
   ACFT_TASKS.part_no_oem,
   ACFT_TASKS.serial_no_oem,
   ACFT_TASKS.task_cd,
   ACFT_TASKS.last_done_dt AS ACTUAL_END_DATE,
   ROUND(DECODE(hours_usage.tsn_qt,0,NULL,HOURS_USAGE.TSN_QT), 2) AS HOURS_TSN,
   -- NVL(ROUND(hours_usage.tsn_qt, 2), 0) AS HOURS_TSN,
   -- NVL(ROUND(hours_usage.tso_qt, 2), 0) AS HOURS_TSO,
   -- NVL(ROUND(hours_usage.tsi_qt, 2), 0) AS HOURS_TSI,
   CAST(DECODE(cyc_usage.tsn_qt, 0, NULL, CYC_USAGE.TSN_QT) AS NUMBER) AS CYCLES_TSN,
   -- NVL(cyc_usage.tsn_qt, 0) AS CYCLES_TSN,
   -- NVL(cyc_usage.tso_qt, 0) AS CYCLES_TSO,
   -- NVL(cyc_usage.tsi_qt, 0) AS CYCLES_TSI,
   acft_tasks.ext_key_sdesc AS EXTERNAL_REFERENCE,
   inv_class,
   ACFT_TASKS.last_done_task_cd AS TASK_CODE_ACCOMP 
FROM (
SELECT 
   h_inv.assmbl_cd AS assmbl, 
   inv.inv_class_cd AS inv_class,
   task_task.etl_strt_dttm,
   evt_event.event_db_id,
   evt_event.event_id,
   eqp_assmbl_bom.assmbl_cd as eqp_as,
            CASE WHEN task_task.task_cd like '%-A' AND task_task.tasK_class_cd <> 'OVHL'
              THEN SUBSTR(task_task.task_cd,1,length(task_task.task_cd)-2)
              WHEN task_task.task_cd like '%-B' AND task_task.tasK_class_cd <> 'OVHL'
              THEN SUBSTR(task_task.task_cd,1,length(task_task.task_cd)-2)
              WHEN task_task.task_cd like '%-C' AND task_task.tasK_class_cd <> 'OVHL'
              THEN SUBSTR(task_task.task_cd,1,length(task_task.task_cd)-2)
              WHEN task_task.task_cd like '%-D' AND task_task.tasK_class_cd <> 'OVHL'
              THEN SUBSTR(task_task.task_cd,1,length(task_task.task_cd)-2)
              WHEN task_task.task_cd like '%-E' AND task_task.tasK_class_cd <> 'OVHL'
              THEN SUBSTR(task_task.task_cd,1,length(task_task.task_cd)-2)
              ELSE task_task.task_cd END AS TASK_CD, -- JM 1106 truncating split requirements 
   -- task_task.task_cd,
   task_task.task_class_cd,
   task_task.recurring_task_bool,
   --inv_inv.serial_no_oem,
   --eqp_part_no.part_no_oem,
   DECODE(inv.inv_class_cd, 'SYS', h_part.part_no_oem, inv_part.part_no_oem) AS PART_NO_OEM, 
   DECODE(inv.inv_class_cd, 'SYS', h_inv.serial_no_oem, inv.serial_no_oem) AS SERIAL_NO_OEM,
   evt_event.event_dt as last_done_dt,
   task_task.ext_key_sdesc,
   SUBSTR(evt_stage.stage_note, 15,  (INSTR(evt_stage.stage_note, ',', 1) - 15)) AS last_done_task_cd
FROM dtl_tmx_task_task task_task
   INNER JOIN dtl_tmx_sched_stask sched_stask ON
      sched_stask.etl_strt_dttm = task_task.etl_strt_dttm AND
      sched_stask.task_db_id = task_task.task_db_id AND
      sched_stask.task_id    = task_task.task_id
   INNER JOIN dtl_tmx_inv_inv inv ON
      inv.etl_strt_dttm = sched_stask.etl_strt_dttm AND
      inv.inv_no_db_id = sched_stask.main_inv_no_db_id AND
      inv.inv_no_id    = sched_stask.main_inv_no_id
      LEFT JOIN dtl_tmx_eqp_part_no inv_part ON
      inv_part.etl_strt_dttm = inv.etl_strt_dttm AND
      inv_part.part_no_id = inv.part_no_id AND
      inv_part.part_no_db_id = inv.part_no_db_id
      INNER JOIN dtl_tmx_inv_inv h_inv ON
      h_inv.etl_strt_dttm = inv.etl_strt_dttm AND
      h_inv.inv_no_db_id = inv.h_inv_no_db_id AND
      h_inv.inv_no_id    = inv.h_inv_no_id
  INNER JOIN dtl_tmx_eqp_part_no h_part ON
     h_part.etl_strt_dttm = h_inv.etl_strt_dttm AND
     h_part.part_no_db_id = h_inv.part_no_db_id AND
     h_part.part_no_id    = h_inv.part_no_id
      INNER JOIN dtl_tmx_inv_inv nh_inv ON
      nh_inv.etl_strt_dttm = inv.etl_strt_dttm AND
      nh_inv.inv_no_db_id = inv.nh_inv_no_db_id AND
      nh_inv.inv_no_id    = inv.nh_inv_no_id
     INNER JOIN DTL_TMX_inv_inv inv_assmbl ON
     inv_assmbl.inv_no_db_id  =  inv.assmbl_inv_no_db_id AND
     inv_assmbl.inv_no_id     =  inv.assmbl_inv_no_id
     LEFT JOIN dtl_tmx_eqp_assmbl_bom eqp_assmbl_bom ON
      eqp_assmbl_bom.etl_strt_dttm  = task_task.etl_strt_dttm AND
      eqp_assmbl_bom.assmbl_db_id  = task_task.assmbl_db_id AND
      eqp_assmbl_bom.assmbl_cd     = task_task.assmbl_cd AND
      eqp_assmbl_bom.assmbl_bom_id = task_task.assmbl_bom_id
  -- INNER JOIN dtl_tmx_evt_event evt_event ON
  --    evt_event.etl_strt_dttm = sched_stask.etl_strt_dttm AND
      INNER JOIN dtl_tmx_evt_event evt_event ON
      evt_event.etl_strt_dttm = sched_stask.etl_strt_dttm AND
      evt_event.event_db_id = sched_stask.sched_db_id AND
      evt_event.event_id    = sched_stask.sched_id AND
      evt_event.event_status_cd = 'COMPLETE'
   -- LEFT JOIN dtl_tmx_evt_stage evt_stage ON
   --    evt_event.etl_strt_dttm = evt_stage.etl_strt_dttm AND
     LEFT JOIN dtl_tmx_evt_stage evt_stage ON
       evt_event.etl_strt_dttm = evt_stage.etl_strt_dttm AND
       evt_event.event_db_id = evt_stage.event_db_id AND
       evt_event.event_id    = evt_stage.event_id AND
       evt_stage.stage_reason_cd IN ('WIZCOMP','HISTDATA')
WHERE
(
         (
            (eqp_assmbl_bom.assmbl_cd IN  ('737NG') AND  inv.inv_class_cd IN ('TRK'))
             AND
            (inv.nh_inv_no_id is not null AND nh_inv.nh_inv_no_id IS NOT NULL AND inv.assmbl_cd = '737NG')
         ) -- config slot based tasks
         OR
         (
            (eqp_assmbl_bom.assmbl_cd IN  ('737NG') AND inv.inv_class_cd IN ( 'SYS'))
             AND
            (h_inv.inv_class_cd = 'ACFT' AND inv.assmbl_cd = '737NG')
         ) -- config slot based tasks only on aircraft
         OR
         (
            (eqp_assmbl_bom.assmbl_cd IS NULL AND h_inv.assmbl_cd = '737NG'  AND inv.inv_class_cd ='TRK')
            AND 
            (inv.nh_inv_no_id is not null AND nh_inv.nh_inv_no_id IS NOT NULL AND inv.assmbl_cd = '737NG')
          ) -- PN based tasks for tracked inventory 
          OR
          (
            (eqp_assmbl_bom.assmbl_cd IS NULL AND h_inv.assmbl_cd = '737NG' AND  inv.inv_class_cd = 'SER')
            AND 
            (inv.nh_inv_no_id is not null AND nh_inv.h_inv_no_id IS NOT NULL) 
          ) -- PN based tasks for serialized inventory
)
) ACFT_TASKS
   LEFT OUTER JOIN dtl_tmx_evt_inv_usge hours_usage ON
      hours_usage.etl_strt_dttm = ACFT_TASKS.etl_strt_dttm AND
      hours_usage.event_db_id = ACFT_TASKS.event_db_id AND
      hours_usage.event_id = ACFT_TASKS.event_id AND
      hours_usage.data_type_db_id = 0 AND
      hours_usage.data_type_id = 1 -- Hours
   LEFT OUTER JOIN dtl_tmx_evt_inv_usge cyc_usage ON
      cyc_usage.etl_strt_dttm = ACFT_TASKS.etl_strt_dttm AND
      cyc_usage.event_db_id = ACFT_TASKS.event_db_id AND
      cyc_usage.event_id = ACFT_TASKS.event_id AND
      cyc_usage.data_type_db_id = 0 AND
      cyc_usage.data_type_id = 10 -- Cycles
WHERE acft_tasks.etl_strt_dttm = <etl_start_datetime>



