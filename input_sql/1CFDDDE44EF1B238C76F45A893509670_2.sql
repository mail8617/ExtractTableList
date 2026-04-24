/* 데이터셋 이름 : 쿼리 작성 - 인메모리 */
select	`a11`.`std_dt`  `std_dt`,
	`a12`.`gender_id`  `gender_id`,
	max(`a13`.`gender_nm`)  `gender_nm`,
	sum(`a11`.`sale_amt`)  `WJXBFS1`
from	`mart`.`mf_sale`	`a11`
	join	`mart`.`md_member`	`a12`
	  on 	(`a11`.`member_id` = `a12`.`member_id`)
	join	`mart`.`md_gender`	`a13`
	  on 	(`a12`.`gender_id` = `a13`.`gender_id`)
group by	`a11`.`std_dt`,
	`a12`.`gender_id`

[Analytical engine calculation steps:
]