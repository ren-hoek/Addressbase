create database addressbase;

use addressbase;

/* 
drop tables header;
drop tables blpu;
drop tables classification;
drop tables delivery_point_address;
drop tables lpi;
drop tables application_cross_reference;
drop tables organisation;
drop tables street;
drop tables street_descriptor;
drop tables successor;
drop tables metadata;
drop tables trailer;
*/

/* record_identifier 10 */
create table header(
 record_identifier int2
 ,custodian_name varchar(40)
 ,local_custodian_name int4
 ,process_date date
 ,volume_number int3
 ,entry_date date
 ,time_stamp time
 ,version varchar(7)
 ,file_type varchar(1)
); 

/* record_identifier 21 */
create table blpu (
 record_identifier int2
 ,change_type varchar(1)
 ,pro_order  bigint
 ,uprn bigint
 ,logical_status int1
 ,blpu_state int1
 ,blpu_state_date date
 ,parent_uprn bigint
 ,x_coordinate float
 ,y_cordinate float
 ,rpc int1
 ,local_custodian_code int4
 ,start_date date
 ,end_date date
 ,last_update_date date
 ,entry_date date
 ,postal_address varchar(1)
 ,postcode_locator varchar(8)
 ,multi_occ_count int4
 ,primary key (uprn)
 ,index ind_pc (postcode_locator)
);

/* record_identifier 32 */
create table classification (
 record_identifier int2
 ,change_type varchar(1)
 ,pro_order bigint
 ,uprn bigint
 ,class_key varchar(14)
 ,classification_code varchar(6)
 ,class_scheme varchar(60)
 ,scheme_version float
 ,start_date date
 ,end_date date
 ,last_update_date date
 ,entry_date date
 ,primary key (class_key)
 ,index ind_uprn (uprn) 
);

/* record_identifier 28 */
create table delivery_point_address (
 record_identifier int2
 ,change_type varchar(1)
 ,pro_order bigint
 ,uprn bigint
 ,parent_addressable_uprn bigint
 ,rm_udprn int8
 ,organisation_name varchar(60)
 ,department_name varchar(60)
 ,sub_building_name varchar(30)
 ,building_name varchar(50)
 ,building_number int4
 ,dependent_thoroughfare_name varchar(80)
 ,thoroughfare_name varchar(35)
 ,double_dependent_locality varchar(35)
 ,dependent_locality varchar(35)
 ,post_town varchar(3)
 ,postcode varchar(8)
 ,postcode_type varchar(1)
 ,welsh_dependent_thoroughfare_name varchar(80)
 ,welsh_thoroughfare_name varchar(80)
 ,welsh_double_dependent_locality varchar(35)
 ,welsh_dependent_locality varchar(35)
 ,welsh_post_town varchar(30)
 ,po_box_number varchar(6)
 ,process_date date
 ,start_date date
 ,end_date date
 ,last_update_date date
 ,entry_date date
 ,primary key (rm_udprn)
 ,index ind_uprn (uprn) 
 ,index ind_pc (postcode)
); 

/* record identifier type 24 */
create table lpi (
 record_identifier int2
 ,change_type varchar(1)
 ,pro_order bigint
 ,uprn bigint
 ,lpi_key varchar(14)
 ,language varchar(3)
 ,logical_status int1
 ,start_date date
 ,end_date date
 ,last_update_date date
 ,entry_date date
 ,sao_start_number int4
 ,sao_start_suffix varchar(2)
 ,sao_end_number int4
 ,sao_end_suffix varchar(2)
 ,sao_text varchar(90)
 ,pao_start_number int4
 ,pao_start_suffix varchar(2)
 ,pao_end_number int4
 ,pao_end_suffix varchar(2)
 ,pao_text varchar(90)
 ,usrn int8
 ,usrn_match_indicator varchar(1)
 ,area_name varchar(35)
 ,level varchar(30)
 ,official_flag varchar(1)
 ,primary key (lpi_key)
 ,index ind_uprn (uprn)
);

/* record identifier type 31 */
create table organisation (
 record_identifier int2
 ,change_type varchar(1)
 ,pro_order bigint
 ,uprn bigint
 ,org_key varchar(14)
 ,organisation varchar(100)
 ,legal_name char(60)
 ,start_date date
 ,end_date date
 ,last_update_date date
 ,entry_date date
 ,primary key (org_key)
 ,index ind_uprn (uprn)
);

/* record identifier type 23 */
create table  application_cross_reference (
 record_identifer int2
 ,change_type varchar(1)
 ,pro_order bigint
 ,uprn bigint
 ,xref_key varchar(14)
 ,cross_reference varchar(50)
 ,version int3
 ,source varchar(6)
 ,start_date date
 ,end_date date
 ,last_update_date date
 ,entry_date date
 ,primary key (xref_key)
 ,index ind_uprn (uprn)
);

/* record identifier type 11 */
create table street (
 record_identifier int2
 ,change_type varchar(1)
 ,pro_order smallint
 ,usrn int8
 ,record_type int1
 ,swa_org_ref_naming int4
 ,state int1
 ,state_date date
 ,street_surface int1
 ,street_classification int2
 ,version int3
 ,street_start_date date
 ,street_end_date date
 ,last_update_date date
 ,record_entry_date date
 ,street_start_x float
 ,street_start_y float
 ,street_end_x float
 ,street_end_y float
 ,street_tolerance int3
 ,primary key (usrn)
);

/* record identifier type 15 */
create table street_descriptor (
 record_identifier int2
 ,change_type varchar(1)
 ,pro_order bigint
 ,usrn int8
 ,street_description varchar(100)
 ,locality_name varchar(35)
 ,town_name varchar(30)
 ,administrative_area varchar(30)
 ,language varchar(3)
 ,index ind_usrn (usrn)
);

/* record identifier type 30 */
create table successor (
 record_identifier int2
 ,change_type varchar(1)
 ,pro_order bigint
 ,uprn bigint
 ,succ_key varchar(14)
 ,start_date date
 ,end_date date
 ,last_update_date date
 ,entry_date date
 ,successor bigint
 ,primary key (succ_key)
 ,index ind_uprn (uprn)
);

/* record identifier type 29 */
create table metadata (
 record_identifier int2
 ,gaz_name varchar(60)
 ,gaz_scope varchar(60)
 ,ter_of_use varchar(60)
 ,linked_date varchar(100)
 ,gaz_owner varchar(15)
 ,ngaz_freq varchar(1)
 ,custodian_name varchar(40)
 ,custodian_uprn bigint
 ,local_custodian_code int4
 ,co_ord_system varchar(40)
 ,co_ord_unit varchar(10)
 ,meta_date date
 ,class_scheme varchar(40)
 ,gaz_date date
 ,language varchar(3)
 ,character_set varchar(30)
); 
  
/* record identifier type 99 */
create table trailer (
 record_identifier int2
 ,next_volume_number int3
 ,record_count smallint
 ,entry_date date
 ,time_stamp time
); 

/* run python script to import data into addressbase */

select 
 * 
from
  blpu as a
 inner join 
  lpi as b
 on a.uprn  = b.uprn
 inner join
  street_descriptor as c
 on b.usrn = c.usrn
where postcode_locator = '*'
 and pao_start_number = *
 and a.end_date is null;

