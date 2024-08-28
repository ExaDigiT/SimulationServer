"""
Makeshift "orm" declaring the Druid tables.
"""

from sqlalchemy import Table, Column, String, Integer, Double, TIMESTAMP
import sqlalchemy as sqla

metadata = sqla.MetaData()

scheduler_sim_job = Table(
    "sens-svc-event-exadigit-scheduler-sim-job",
    metadata,
    Column("__time", TIMESTAMP),
    Column("sim_id", String),
    Column("job_id", String),
    Column("name", String),
    Column("node_count", Integer),
    Column("time_submission", String),
    Column("time_limit", Integer),
    Column("time_start", String),
    Column("time_end", String),
    Column("state_current", String),
    Column("node_ranges", String),
    Column("xnames", String),
)

cooling_sim_cdu = Table(
    "svc-ts-exadigit-cooling-sim-cdu",
    metadata,
    Column("__time", TIMESTAMP),
    Column("sim_id", String),
    Column("xname", String),
    Column("row", Integer),
    Column("col", Integer),
    Column("rack_1_power", Double),
    Column("rack_2_power", Double),
    Column("rack_3_power", Double),
    Column("total_power", Double),
    Column("rack_1_loss", Double),
    Column("rack_2_loss", Double),
    Column("rack_3_loss", Double),
    Column("total_loss", Double),
    Column("work_done_by_cdup", Double),
    Column("rack_return_temp", Double),
    Column("rack_supply_temp", Double),
    Column("rack_supply_pressure", Double),
    Column("rack_return_pressure", Double),
    Column("rack_flowrate", Double),
    Column("htw_ctw_flowrate", Double),
    Column("htwr_htws_ctwr_ctws_pressure", Double),
    Column("htwr_htws_ctwr_ctws_temp", Double),
    Column("power_cunsumption_htwps", Double),
    Column("power_consumption_ctwps", Double),
    Column("power_consumption_fan", Double),
    Column("htwp_speed", Double),
    Column("nctwps_staged", Double),
    Column("nhtwps_staged", Double),
    Column("pue_output", Double),
    Column("nehxs_staged", Double),
    Column("ncts_staged", Double),
    Column("facility_return_temp", Double),
    Column("facility_supply_temp", Double),
    Column("facility_supply_pressure", Double),
    Column("facility_return_pressure", Double),
    Column("cdu_loop_bypass_flowrate", Double),
    Column("facility_flowrate", Double),
)

scheduler_sim_job_power_history = Table(
    "svc-ts-exadigit-scheduler-sim-job-power-history",
    metadata,
    Column("__time", TIMESTAMP),
    Column("sim_id", String),
    Column("job_id", String),
    Column("power", Double),
)

scheduler_sim_system = Table(
    "svc-ts-exadigit-scheduler-sim-system",
    metadata,
    Column("__time", TIMESTAMP),
    Column("sim_id", String),
    Column("down_nodes", String),
    Column("num_samples", Integer),
    Column("jobs_completed", Integer),
    Column("jobs_running", Integer),
    Column("jobs_pending", Integer),
    Column("throughput", Double),
    Column("average_power", Double),
    Column("min_loss", Double),
    Column("average_loss", Double),
    Column("max_loss", Double),
    Column("system_power_efficiency", Double),
    Column("total_energy_consumed", Double),
    Column("carbon_emissions", Double),
    Column("total_cost", Double),
    Column("p_flops", Double),
    Column("g_flops_w", Double),
    Column("system_util", Double),
)

sim = Table(
    "svc-event-exadigit-sim",
    metadata,
    Column("__time", TIMESTAMP),
    Column("id", String),
    Column("user", String),
    Column("state", String),
    Column("error_messages", String),
    Column("start", String),
    Column("end", String),
    Column("execution_start", String),
    Column("execution_end", String),
    Column("config", String),
)
