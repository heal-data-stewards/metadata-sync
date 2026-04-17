/* 
Author: Kiryl
Reviewer: Sabrina
Date Last Updated: 2024/09/24
*/


-- Allow for table updates --
SET SQL_SAFE_UPDATES = 0; 

-- Clear old data and add res_prg column --
delete from research_networks r;
alter table research_networks
add res_prg varchar(135);

-- Add current awards data --
insert into research_networks (appl_id, res_prg)
select appl_id, res_prg
from awards;

-- Update res_net based on reference table --
update research_networks
left join res_net_ref_table on research_networks.res_prg = res_net_ref_table.res_prg
set research_networks.res_net = res_net_ref_table.res_net;

-- Drop res_prg column --
alter table research_networks drop column res_prg;

-- Update the override values --
update research_networks
left join res_net_override on research_networks.appl_id = res_net_override.appl_id
	set research_networks.res_net = res_net_override.res_net 
		where research_networks.appl_id = res_net_override.appl_id;

-- Update the override flag -- 
update research_networks
set research_networks.res_net_override_flag = (
	case
		when exists (select * from res_net_override where research_networks.appl_id = res_net_override.appl_id) then 1
        else 0
	end
    );      
        
        
SET SQL_SAFE_UPDATES = 1; 

/*
create view `test` as 
select res_net_override.appl_id AS override_ids 
from res_net_override 
where research_networks.appl_id = res_net_override.appl_id ;
*/
