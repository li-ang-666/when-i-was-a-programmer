drop table if exists `prism_shareholder_path`.`ratio_path_company_new`;
CREATE TABLE if not exists `prism_shareholder_path`.`ratio_path_company_new` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键id',
  -- 公司
  `company_id` bigint(20) unsigned NOT NULL DEFAULT '0' COMMENT '公司id',
  `company_name` varchar(255) NOT NULL DEFAULT '' COMMENT '公司名称',
  `company_is_listed` tinyint NOT NULL DEFAULT '0' COMMENT '公司是否上市',
  `company_uscc` varchar(255) NOT NULL DEFAULT '' COMMENT '统一社会信用代码',
  `company_org_type` varchar(255) NOT NULL DEFAULT '' COMMENT 'company_index.org_type',
  `company_entity_property` varchar(255) NOT NULL DEFAULT '' COMMENT 'tyc_entity_general_property_reference.entity_property',
  -- 股东
  `shareholder_entity_type` tinyint NOT NULL DEFAULT '0' COMMENT '1-company,2-human',
  `shareholder_id` varchar(50) NOT NULL DEFAULT '' COMMENT '股东id,人存human_pid,公司存储company_gid',
  `shareholder_name` varchar(255) NOT NULL DEFAULT '' COMMENT '股东名称',
  `shareholder_name_id` bigint(20) unsigned NOT NULL DEFAULT '0' COMMENT '股东内链中的human_name_id',
  `shareholder_master_company_id` bigint(20) unsigned NOT NULL DEFAULT '0' COMMENT '股东内链中的company_id',
  `shareholder_first_appear_level` int NOT NULL DEFAULT '0' COMMENT '股东第一次出现的层级',
  `shareholder_last_appear_level` int NOT NULL DEFAULT '0' COMMENT '股东最后一次出现的层级',
  -- 投资
  `is_direct_shareholder` tinyint NOT NULL DEFAULT '0' COMMENT '是否为直接股东',
  `investment_ratio_direct` decimal(26,6) NOT NULL DEFAULT '0' COMMENT '直接持股比例',
  `max_deliver` decimal(26,6) NOT NULL DEFAULT '0' COMMENT '最大直接持股传递',
  `investment_ratio_total` decimal(26,6) NOT NULL DEFAULT '0' COMMENT '总持股比例',
  `equity_holding_path` longtext COMMENT '持股路径json',
  `is_end` tinyint NOT NULL DEFAULT '0' COMMENT '是否是某条持股路径的终点',
  -- 其他
  `is_big_shareholder` tinyint NOT NULL DEFAULT '0' COMMENT '是否大股东',
  `is_controlling_shareholder` tinyint NOT NULL DEFAULT '0' COMMENT '是否控股股东',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'create_time',
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'update_time',
  PRIMARY KEY (`id`),
  KEY `idx_company` (`company_id`),
  KEY `idx_shareholder` (`shareholder_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='投资路径结果表';