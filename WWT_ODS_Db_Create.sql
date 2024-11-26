/*
Drop table order_detail
Drop table order
Drop table product 
Drop table employee_territory
Drop table territory
Drop table supplier
drop table employee
drop table customer
drop table address
drop table region
drop table site
*/

CREATE TABLE [site] (
  [site_id] tinyint,
  [name] varchar(50),
  [date_created] datetime,
  [last_update] datetime,
  PRIMARY KEY ([site_id])
);

CREATE TABLE [region] (
  [ODS_region_id] smallint identity(1,1),
  [orig_region_id] smallint,
  [description] varchar(100),
  [date_created] datetime,
  [last_update] datetime,
  PRIMARY KEY ([ODS_region_id])
);

CREATE TABLE [address] (
  [ODS_address_id] int identity(1,1),
  [orig_address_id] int,
  [address1] varchar(50),
  [address2] varchar(50),
  [city] varchar(50),
  [state] varchar(50),
  [postal_code] varchar(10),
  [country] varchar(50),
  [date_created] datetime,
  [last_update] datetime,
  [site_id] tinyint,
  PRIMARY KEY ([ODS_address_id]),
  CONSTRAINT [FK_address_site_id]
    FOREIGN KEY ([site_id])
      REFERENCES [site]([site_id])
);

CREATE TABLE [customer] (
  [ODS_customer_id] int identity(1,1),
  [orig_customer_id] varchar(5),
  [company_name] varchar(50),
  [contact_name] varchar(50),
  [contact_job_title] varchar(50),
  [phone] varchar(20),
  [ODS_address_id] int,
  [ODS_delivery_address_id] int,
  [date_created] datetime,
  [last_update] datetime,
  [record_orig_site_id] tinyint,
  PRIMARY KEY ([ODS_customer_id]),
  CONSTRAINT [FK_customer_address_id]
    FOREIGN KEY (ODS_address_id)
      REFERENCES [address] ([ODS_address_id]),
  CONSTRAINT [FK_customer_delivery_address_id]
    FOREIGN KEY (ODS_delivery_address_id)
      REFERENCES [address] ([ODS_address_id]),
  CONSTRAINT [FK_customer_site_id]
    FOREIGN KEY (record_orig_site_id)
      REFERENCES [site]([site_id])
);

CREATE TABLE [employee] (
  [ODS_employee_id] smallint identity(1,1),
  [orig_employee_id] smallint,
  [first_name] varchar(50),
  [last_name] varchar(50),
  [job_title] varchar(50),
  [birth_date] datetime,
  [hire_date] datetime,
  [termination_date] datetime,
  [phone] varchar(20),
  [ODS_address_id] int,
  [email] varchar(50),
  [reports_to] varchar(50),
  [notes] varchar(200),
  [date_created] datetime,
  [last_update] datetime,
  [record_orig_site_id] tinyint,
  PRIMARY KEY ([ODS_employee_id]),
  CONSTRAINT [FK_employee_address_id]
    FOREIGN KEY (ODS_address_id)
      REFERENCES [address] ([ODS_address_id]),
  CONSTRAINT [FK_employee_site_id]
    FOREIGN KEY (record_orig_site_id)
      REFERENCES [site]([site_id])
);

CREATE TABLE [supplier] (
  [ODS_supplier_id] int identity(1,1),
  [orig_supplier_id] int,
  [company_name] varchar(50),
  [contact_name] varchar(50),
  [contact_job_title] varchar(50),
  [phone] varchar(20),
  [website] varchar(100),
  [ODS_address_id] int,
  [date_created] datetime,
  [last_update] datetime,
  [site_id] tinyint,
  PRIMARY KEY ([ODS_supplier_id]),
  CONSTRAINT [FK_supplier_ODS_address_id]
    FOREIGN KEY ([ODS_address_id])
      REFERENCES [address]([ODS_address_id]),
  CONSTRAINT [FK_supplier_site_id]
    FOREIGN KEY ([site_id])
      REFERENCES [site]([site_id])
);

CREATE TABLE [territory] (
  [ODS_territory_id] smallint identity(1,1),
  [orig_territory_id] varchar(5),
  [description] varchar(100),
  [ODS_region_id] smallint,
  [date_created] datetime,
  [last_update] datetime,
  [record_orig_site_id] tinyint,
  PRIMARY KEY ([ODS_territory_id]),
  CONSTRAINT [FK_territory_site_id]
    FOREIGN KEY ([record_orig_site_id])
      REFERENCES [site]([site_id]),
  CONSTRAINT [FK_territory_ODS_region_id]
    FOREIGN KEY ([ODS_region_id])
      REFERENCES [region]([ODS_region_id])
);

CREATE TABLE [employee_territory] (
  [ODS_employee_id] smallint,
  [ODS_territory_id] smallint,
  [date_created] datetime,
  PRIMARY KEY ([ODS_employee_id], [ODS_territory_id]),
  CONSTRAINT [FK_employee_territory_ODS_employee_id]
    FOREIGN KEY ([ODS_employee_id])
      REFERENCES [employee]([ODS_employee_id]),
  CONSTRAINT [FK_employee_territory_ODS_territory_id]
    FOREIGN KEY ([ODS_territory_id])
      REFERENCES [territory]([ODS_territory_id])
);

CREATE TABLE [product] (
  [ODS_product_id] int identity(1,1),
  [orig_product_id] int,
  [name] varchar(100),
  [category_name] varchar(50),
  [category_description] varchar(max),
  [ODS_supplier_id] int,
  [color] varchar(50),
  [size] varchar(50),
  [quantity_per_unit] varchar(20),
  [unit_price] decimal(5,2),
  [wholesale_price] decimal(5,2),
  [units_in_stock] int,
  [units_on_order] int,
  [reorder_level] int,
  [target_stock_level] int,
  [bin_location] varchar(50),
  [discontinued] bit,
  [date_created] datetime,
  [last_update] datetime,
  [site_id] tinyint,
  PRIMARY KEY ([ODS_product_id]),
  CONSTRAINT [FK_product_ODS_supplier_id]
    FOREIGN KEY ([ODS_supplier_id])
      REFERENCES [supplier]([ODS_supplier_id]),
  CONSTRAINT [FK_product_site_id]
    FOREIGN KEY ([site_id])
      REFERENCES [site]([site_id])
);

CREATE TABLE [order] (
  [ODS_order_id] int identity(1,1),
  [orig_order_id] int,
  [ODS_customer_id] int,
  [ODS_employee_id] smallint,
  [ODS_address_id] int,
  [order_date] datetime,
  [ship_date] datetime,
  [required_date] datetime,
  [order_number] varchar(50),
  [ship_via] varchar(50),
  [shipping_company_phone] varchar(20),
  [freight] money,
  [date_created] datetime,
  [last_update] datetime,
  [site_id] tinyint,
  PRIMARY KEY ([ODS_order_id]),
  CONSTRAINT [FK_order_ODS_customer_id]
    FOREIGN KEY ([ODS_customer_id])
      REFERENCES [customer]([ODS_customer_id]),
  CONSTRAINT [FK_order_ODS_employee_id]
    FOREIGN KEY ([ODS_employee_id])
      REFERENCES [employee]([ODS_employee_id]),
  CONSTRAINT [FK_order_ODS_address_id]
    FOREIGN KEY ([ODS_address_id])
      REFERENCES [address]([ODS_address_id]),
  CONSTRAINT [FK_order_site_id]
    FOREIGN KEY ([site_id])
      REFERENCES [site]([site_id])
);

CREATE TABLE [order_detail] (
  [ODS_order_detail_id] int identity(1,1),
  [orig_order_detail_id] int,
  [ODS_order_id] int,
  [ODS_product_id] int,
  [quantity_per_unit] int,
  [unit_price] decimal,
  [tax_rate] decimal,
  [item_picked_date] datetime,
  [discount] decimal,
  [date_created] datetime,
  [last_update] datetime,
  [site_id] tinyint,
  PRIMARY KEY ([ODS_order_detail_id]),
  CONSTRAINT [FK_order_detail_ODS_order_id]
    FOREIGN KEY ([ODS_order_id])
      REFERENCES [order]([ODS_order_id]),
  CONSTRAINT [FK_order_detail_ODS_product_id]
    FOREIGN KEY ([ODS_product_id])
      REFERENCES [product]([ODS_product_id]),
  CONSTRAINT [FK_order_detail_site_id]
    FOREIGN KEY ([site_id])
      REFERENCES [site]([site_id])
);