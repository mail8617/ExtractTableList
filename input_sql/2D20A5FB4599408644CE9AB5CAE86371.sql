select	`a11`.`std_dt`  `std_dt`,
	`a12`.`gender_id`  `gender_id`,
	max(`a13`.`gender_nm`)  `gender_nm`,
	sum(`a11`.`sale_amt`)  `WJXBFS1`
from	`mart`.`mf_sale`	`a11`
	join	`mart`.`md_member`	`a12`
	  on 	(`a11`.`member_id` = `a12`.`member_id`)
	join	`mart`.`md_gender`	`a13`
	  on 	(`a12`.`gender_id` = `a13`.`gender_id`)
where	(`a11`.`std_dt` = #0[값 (날짜)]
 and `a12`.`age_id` = #0[값 (숫자)]
 and `a12`.`gender_id` = '#0'[값 (텍스트)]
)
group by	`a11`.`std_dt`,
	`a12`.`gender_id`