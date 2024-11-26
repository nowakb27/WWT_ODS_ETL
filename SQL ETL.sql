/****************  
* PROCEDURE ETL_LoadSite
* AUTHOR: bnowak
* DATE CREATED: 10/20/2024
*
* THIS PROCEDURE LOADS VALUES INTO THE SITE TABLE
* IT INSERTS ONLY NEW VALUES. IT DOES NOT DO ANY UPDATES OR DELETES
*
* CHANGE LOG
* --------------
* 11/24 - corrected error when ran twice
*
****************/
ALTER PROCEDURE ETL_LoadSite
AS
BEGIN
	INSERT INTO site(site_id, name, date_created, last_update)
	SELECT *
	FROM (
		VALUES 
		(1, 'SouthwindSales', getDate(), NULL),
		(2, 'NW_Traders_Sales', getDate(), NULL)
	) AS sites(site_id, name, date_created, last_update)
	WHERE NOT EXISTS (
		SELECT 1 
		FROM site 
		WHERE site.site_id = sites.site_id
	)
END


GO
/****************  
* PROCEDURE ETL_LoadRegion
* AUTHOR: bnowak
* DATE CREATED: 10/20/2024
*
* THIS PROCEDURE LOADS THE ODS REGION TABLE FROM THE STG_REGION TABLE
* IT INSERTS NEW RECORDS. IT ALSO UPDATES WHEN THERE IS A CHANGE TO A REGION
*
* CHANGE LOG
* --------------
*
****************/
ALTER PROCEDURE ETL_LoadRegion
AS
BEGIN
	UPDATE region
	SET description = src_reg.RegionDescription, 
	last_update = getDate()
	FROM Stg_Region src_reg
	JOIN region tgt_reg
	ON src_reg.RegionID = tgt_reg.orig_region_id
	WHERE NOT (ISNULL(src_reg.RegionDescription, '') = ISNULL(tgt_reg.description, ''))

	INSERT INTO region(orig_region_id, description, date_created, last_update)
	SELECT DISTINCT src_reg.RegionID,
	src_reg.RegionDescription, 
	getDate() as date_created,
	NULL AS last_update
	FROM Stg_Region src_reg
	LEFT JOIN region tgt_reg
	ON src_reg.RegionID = tgt_reg.orig_region_id
	WHERE tgt_reg.ODS_region_id IS NULL
END


GO
/****************  
* PROCEDURE ETL_LoadAddress
* AUTHOR: bnowak
* DATE CREATED: 10/20/2024
*
* THIS PROCEDURE LOADS THE ODS ADDRESS TABLE FROM THE STG_ADDRESS, STG_CUSTOMERS, STG_EMPLOYEES, STG_SUPPLIERS, AND STG_ORDERS TABLES
* IT INSERTS ONLY NEW RECORDS. IT ALSO UPDATES WHEN THERE IS A CHANGE TO AN ADDRESS
*
* CHANGE LOG
* --------------
* 11/24 - changed address1 and address2 to full_address to easily pinpoint duplicates
*
****************/
ALTER PROCEDURE ETL_LoadAddress
AS
BEGIN
UPDATE address
	SET full_address = CONCAT(src_add.AddressLine1, ' ', src_add.AddressLine2),
	city = src_add.City,
	state = src_add.State,
	postal_code = src_add.ZipCode,
	last_update = getDate()
	FROM Stg_Address src_add
	JOIN address tgt_add
	ON src_add.AddressId = tgt_add.orig_address_id
	WHERE NOT (ISNULL(CONCAT(src_add.AddressLine1, ' ', src_add.AddressLine2), '') = ISNULL(tgt_add.full_address, '')
		AND ISNULL(src_add.City, '') = ISNULL(tgt_add.city, '')
		AND ISNULL(src_add.State, '') = ISNULL(tgt_add.state, '')
		AND ISNULL(src_add.ZipCode, '') = ISNULL(tgt_add.postal_code, '')
	)
	INSERT INTO address(orig_address_id, full_address, city, state, postal_code, country, date_created, last_update, site_id)
	SELECT DISTINCT src_add.AddressId,
	CONCAT(src_add.AddressLine1, ' ', src_add.AddressLine2) AS full_address,
	src_add.City,
	src_add.State,
	src_add.ZipCode,
	NULL AS country,
	getDate() as date_created,
	NULL AS last_update,
	src_add.RecordOrigSiteId
	FROM Stg_Address src_add
	LEFT JOIN address tgt_add
	ON src_add.AddressId = tgt_add.orig_address_id
	WHERE tgt_add.ODS_address_id IS NULL
	UNION
	SELECT DISTINCT NULL AS orig_address_id,
	src_cus.Address,
	src_cus.City,
	src_cus.Region,
	src_cus.PostalCode,
	src_cus.Country,
	getDate() as date_created,
	NULL AS last_update,
	src_cus.SiteId
	FROM Stg_Customers src_cus
	LEFT JOIN address tgt_add
	ON src_cus.Address = tgt_add.full_address 
	WHERE tgt_add.ODS_address_id IS NULL
	UNION
	SELECT DISTINCT NULL AS orig_address_id,
	src_emp.Address,
	src_emp.City,
	src_emp.StateOrRegion,
	src_emp.PostalCode,
	src_emp.Country,
	getDate() as date_created,
	NULL AS last_update,
	src_emp.SiteId
	FROM Stg_Employees src_emp
	LEFT JOIN address tgt_add
	ON src_emp.Address = tgt_add.full_address 
	WHERE src_emp.Address IS NOT NULL 
		AND tgt_add.ODS_address_id IS NULL
	UNION
	SELECT DISTINCT NULL AS orig_address_id,
	src_sup.Address,
	src_sup.City,
	src_sup.StateOrRegion,
	src_sup.PostalCode,
	src_sup.Country,
	getDate() as date_created,
	NULL AS last_update,
	src_sup.SiteId
	FROM Stg_Suppliers src_sup
	LEFT JOIN address tgt_add
	ON src_sup.Address = tgt_add.full_address 
	WHERE tgt_add.ODS_address_id IS NULL
	UNION
	SELECT DISTINCT NULL AS orig_address_id,
	src_ord.ShipAddress,
	src_ord.ShipCity,
	src_ord.ShipStateOrRegion,
	src_ord.ShipPostalCode,
	src_ord.ShipCountry,
	getDate() as date_created,
	NULL AS last_update,
	src_ord.SiteId
	FROM Stg_Orders src_ord
	LEFT JOIN address tgt_add
	ON src_ord.ShipAddress = tgt_add.full_address AND src_ord.ShipCountry = tgt_add.country
	WHERE tgt_add.ODS_address_id IS NULL
END


GO
/****************  
* PROCEDURE ETL_LoadCustomer
* AUTHOR: bnowak
* DATE CREATED: 10/20/2024
*
* THIS PROCEDURE LOADS THE ODS CUSTOMER TABLE FROM THE STG_CUSTOMERS, AND STG_CUSTOMER TABLES
* IT INSERTS NEW RECORDS. IT ALSO UPDATES WHEN THERE IS A CHANGE TO A CUSTOMER
*
* CHANGE LOG
* --------------
* 11/24 - corrected address data in customer table
*
****************/
ALTER PROCEDURE ETL_LoadCustomer
AS
BEGIN
	UPDATE customer
	SET company_name = src_cus.CustomerName,
	contact_name = src_cus.ContactName,
	phone = src_cus.PhoneNumber,
	last_update = getDate()
	FROM Stg_Customer src_cus
	JOIN customer tgt_cus
	ON CAST(src_cus.CustomerId AS varchar(5)) = tgt_cus.orig_customer_id
	WHERE NOT (ISNULL(src_cus.CustomerName, '') = ISNULL(tgt_cus.company_name, '')
		AND ISNULL(src_cus.ContactName, '') = ISNULL(tgt_cus.contact_name, '')
		AND ISNULL(src_cus.PhoneNumber, '') = ISNULL(tgt_cus.phone, '')
	)
	UPDATE customer
	SET company_name = src_cuss.CompanyName,
	contact_name = src_cuss.ContactName,
	contact_job_title = src_cuss.ContactTitle,
	phone = src_cuss.Phone,
	last_update = getDate()
	FROM Stg_Customers src_cuss
	JOIN customer tgt_cus
	ON src_cuss.CustomerId = tgt_cus.orig_customer_id
	WHERE NOT (ISNULL(src_cuss.CompanyName, '') = ISNULL(tgt_cus.company_name, '')
		AND ISNULL(src_cuss.ContactName, '') = ISNULL(tgt_cus.contact_name, '')
		AND ISNULL(src_cuss.ContactTitle, '') = ISNULL(tgt_cus.contact_job_title, '')
		AND ISNULL(src_cuss.Phone, '') = ISNULL(tgt_cus.phone, '')
	)

	INSERT INTO customer(orig_customer_id, company_name, contact_name, contact_job_title, phone, ODS_address_id, ODS_delivery_address_id, date_created, last_update, record_orig_site_id)
	SELECT DISTINCT CAST(src_cus.CustomerId AS varchar(5)) AS orig_customer_id,
	src_cus.CustomerName,
	src_cus.ContactName,
	NULL AS contact_job_title,
	src_cus.PhoneNumber,
	src_add.ODS_address_id,
	src_add2.ODS_address_id,
	getDate() AS date_created,
	NULL AS last_update,
	src_cus.RecordOrigSiteId
	FROM Stg_Customer src_cus
	LEFT JOIN Stg_Address src_addb
	ON src_cus.BillingAddressId = src_addb.AddressId 
	LEFT JOIN Stg_Address src_addd
	ON src_cus.DeliveryAddressId = src_addd.AddressId
	LEFT JOIN address src_add
	ON CONCAT(src_addb.AddressLine1, ' ', src_addb.AddressLine2) = src_add.full_address AND src_add.site_id = 1
	LEFT JOIN address src_add2
	ON CONCAT(src_addd.AddressLine1, ' ', src_addd.AddressLine2) = src_add2.full_address AND src_add2.site_id = 1
	LEFT JOIN customer tgt_cus
	ON CAST(src_cus.CustomerId AS varchar(5)) = tgt_cus.orig_customer_id
	WHERE tgt_cus.ODS_customer_id IS NULL
	UNION
	SELECT DISTINCT src_cuss.CustomerID,
	src_cuss.CompanyName,
	src_cuss.ContactName,
	src_cuss.ContactTitle,
	src_cuss.Phone,
	src_add.ODS_address_id,
	src_add.ODS_address_id,
	getDate() as date_created,
	NULL AS last_update,
	src_cuss.SiteId
	FROM Stg_Customers src_cuss
	LEFT JOIN address src_add
	ON src_cuss.Address = src_add.full_address
	LEFT JOIN customer tgt_cus
	ON src_cuss.CustomerID = tgt_cus.orig_customer_id 
	WHERE src_add.site_id = 2
		AND tgt_cus.ODS_customer_id IS NULL
END


GO
/****************  
* PROCEDURE ETL_LoadEmployee
* AUTHOR: bnowak
* DATE CREATED: 10/20/2024
*
* THIS PROCEDURE LOADS THE ODS EMPLOYEE TABLE FROM THE STG_EMPLOYEES, AND STG_SALESPERSON TABLES
* IT INSERTS NEW RECORDS. IT ALSO UPDATES WHEN THERE IS A CHANGE TO AN EMPLOYEE
*
* CHANGE LOG
* --------------
*
****************/
ALTER PROCEDURE ETL_LoadEmployee
AS
BEGIN
	UPDATE employee
	SET first_name = src_sp.FirstName,
	last_name = src_sp.LastName,
	phone = src_sp.PhoneNumber,
	email = src_sp.Email,
	last_update = getDate()
	FROM Stg_SalesPerson src_sp
	JOIN employee tgt_emp
	ON src_sp.SalesPersonId = tgt_emp.orig_employee_id
	WHERE NOT (ISNULL(src_sp.FirstName, '') = ISNULL(tgt_emp.first_name, '')
		AND ISNULL(src_sp.LastName, '') = ISNULL(tgt_emp.last_name, '')
		AND ISNULL(src_sp.PhoneNumber, '') = ISNULL(tgt_emp.phone, '')
		AND ISNULL(src_sp.Email, '') = ISNULL(tgt_emp.email, '')
	)
	UPDATE employee
	SET first_name = src_emp.FirstName,
	last_name = src_emp.LastName,
	job_title = src_emp.JobTitle,
	birth_date = src_emp.BirthDate,
	hire_date = src_emp.HireDate,
	termination_date = src_emp.TerminationDate,
	phone = src_emp.HomePhone,
	email = src_emp.EmailAddress,
	reports_to = src_emp.ReportsTo,
	notes = src_emp.Notes,
	last_update = getDate()
	FROM Stg_Employees src_emp
	JOIN employee tgt_emp
	ON src_emp.EmployeeID = tgt_emp.orig_employee_id
	WHERE NOT (ISNULL(src_emp.FirstName, '') = ISNULL(tgt_emp.first_name, '')
		AND ISNULL(src_emp.LastName, '') = ISNULL(tgt_emp.last_name, '')
		AND ISNULL(src_emp.JobTitle, '') = ISNULL(tgt_emp.job_title, '')
		AND ISNULL(src_emp.BirthDate, '') = ISNULL(tgt_emp.birth_date, '')
		AND ISNULL(src_emp.HireDate, '') = ISNULL(tgt_emp.hire_date, '')
		AND ISNULL(src_emp.TerminationDate, '') = ISNULL(tgt_emp.termination_date, '')
		AND ISNULL(src_emp.HomePhone, '') = ISNULL(tgt_emp.phone, '')
		AND ISNULL(src_emp.EmailAddress, '') = ISNULL(tgt_emp.email, '')
		AND ISNULL(src_emp.ReportsTo, '') = ISNULL(tgt_emp.reports_to, '')
		AND ISNULL(src_emp.Notes, '') = ISNULL(tgt_emp.notes, '')
	)

	INSERT INTO employee(orig_employee_id, first_name, last_name, job_title, birth_date, hire_date, termination_date, phone, ODS_address_id, email, reports_to, notes, date_created, last_update, record_orig_site_id)
	SELECT DISTINCT src_sp.SalesPersonId,
	src_sp.FirstName,
	src_sp.LastName,
	NULL AS job_title,
	NULL AS birth_date,
	NULL AS hire_date,
	CASE
		WHEN src_sp.IsActive = 0
		THEN getDate() ELSE NULL
	END AS termination_date,
	src_sp.PhoneNumber,
	NULL AS ODS_address_id,
	src_sp.Email,
	NULL AS reports_to,
	NULL AS notes,
	getDate() AS date_created,
	NULL AS last_update,
	src_sp.RecordOrigSiteId
	FROM Stg_SalesPerson src_sp
	LEFT JOIN employee tgt_emp
	ON src_sp.SalesPersonId = tgt_emp.orig_employee_id
	WHERE tgt_emp.ODS_employee_id IS NULL
	UNION
	SELECT DISTINCT src_emp.EmployeeID,
	src_emp.FirstName,
	src_emp.LastName,
	src_emp.JobTitle,
	src_emp.BirthDate,
	src_emp.HireDate,
	src_emp.TerminationDate,
	src_emp.HomePhone,
	src_add.ODS_address_id,
	src_emp.EmailAddress,
	src_emp.ReportsTo,
	src_emp.Notes,
	getDate() AS date_created,
	NULL AS last_update,
	src_emp.SiteId
	FROM Stg_Employees src_emp
	LEFT JOIN address src_add
	ON src_emp.Address = src_add.full_address
	LEFT JOIN employee tgt_emp
	ON src_emp.EmployeeID = tgt_emp.orig_employee_id
	WHERE tgt_emp.ODS_employee_id IS NULL
END


GO
/****************  
* PROCEDURE ETL_LoadSupplier
* AUTHOR: bnowak
* DATE CREATED: 10/20/2024
*
* THIS PROCEDURE LOADS THE ODS SUPPLIER TABLE FROM THE STG_SUPPLIERS, AND STG_SUPPLIER TABLES
* IT INSERTS NEW RECORDS. IT ALSO UPDATES WHEN THERE IS A CHANGE TO A SUPPLIER
*
* CHANGE LOG
* --------------
*
****************/
ALTER PROCEDURE ETL_LoadSupplier
AS
BEGIN
	UPDATE supplier
	SET company_name = src_sup.SupplierName,
	contact_name = src_sup.ContactName,
	phone = src_sup.PhoneNumber,
	website = src_sup.Website,
	last_update = getDate()
	FROM Stg_Supplier src_sup
	JOIN supplier tgt_sup
	ON src_sup.SupplierId = tgt_sup.orig_supplier_id
	WHERE NOT (ISNULL(src_sup.SupplierName, '') = ISNULL(tgt_sup.company_name, '')
		AND ISNULL(src_sup.ContactName, '') = ISNULL(tgt_sup.contact_name, '')
		AND ISNULL(src_sup.PhoneNumber, '') = ISNULL(tgt_sup.phone, '')
		AND ISNULL(src_sup.Website, '') = ISNULL(tgt_sup.website, '')
	)
	UPDATE supplier
	SET company_name = src_sups.CompanyName,
	contact_name = src_sups.ContactName,
	contact_job_title = src_sups.ContactTitle,
	phone = src_sups.Phone,
	last_update = getDate()
	FROM Stg_Suppliers src_sups
	JOIN supplier tgt_sup
	ON src_sups.SupplierID = tgt_sup.orig_supplier_id
	WHERE NOT (ISNULL(src_sups.CompanyName, '') = ISNULL(tgt_sup.company_name, '')
		AND ISNULL(src_sups.ContactName, '') = ISNULL(tgt_sup.contact_name, '')
		AND ISNULL(src_sups.ContactTitle, '') = ISNULL(tgt_sup.contact_job_title, '')
		AND ISNULL(src_sups.Phone, '') = ISNULL(tgt_sup.phone, '')
	)

	INSERT INTO supplier(orig_supplier_id, company_name, contact_name, contact_job_title, phone, website, ODS_address_id, date_created, last_update, site_id)
	SELECT DISTINCT src_sup.SupplierId,
	src_sup.SupplierName,
	src_sup.ContactName,
	NULL AS contact_job_title,
	src_sup.PhoneNumber,
	src_sup.Website,
	src_add.ODS_address_id,
	getDate() AS date_created,
	NULL AS last_update,
	src_sup.SiteId
	FROM Stg_Supplier src_sup
	LEFT JOIN address src_add
	ON src_sup.AddressId = src_add.orig_address_id
	LEFT JOIN supplier tgt_sup
	ON src_sup.SupplierId = tgt_sup.orig_supplier_id
	WHERE tgt_sup.ODS_supplier_id IS NULL
	UNION
	SELECT DISTINCT src_sups.SupplierID,
	src_sups.CompanyName,
	src_sups.ContactName,
	src_sups.ContactTitle,
	src_sups.Phone,
	NULL AS website,
	src_add.ODS_address_id,
	getDate() AS date_created,
	NULL AS last_update,
	src_sups.SiteId
	FROM Stg_Suppliers src_sups
	LEFT JOIN address src_add
	ON src_sups.Address = src_add.full_address
	LEFT JOIN supplier tgt_sup
	ON src_sups.SupplierID = tgt_sup.orig_supplier_id
	WHERE tgt_sup.ODS_supplier_id IS NULL
END


GO
/****************  
* PROCEDURE ETL_LoadTerritory
* AUTHOR: bnowak
* DATE CREATED: 10/20/2024
*
* THIS PROCEDURE LOADS THE ODS TERRITORY TABLE FROM THE STG_TERRITORIES TABLE
* IT INSERTS NEW RECORDS. IT ALSO UPDATES WHEN THERE IS A CHANGE TO A TERRITORY
*
* CHANGE LOG
* --------------
*
****************/
ALTER PROCEDURE ETL_LoadTerritory
AS
BEGIN
	UPDATE territory
	SET description = src_ter.TerritoryDescription,
	last_update = getDate()
	FROM Stg_Territories src_ter
	JOIN territory tgt_ter
	ON src_ter.TerritoryID = tgt_ter.orig_territory_id
	WHERE NOT (ISNULL(src_ter.TerritoryDescription, '') = ISNULL(tgt_ter.description, ''))

	INSERT INTO territory(orig_territory_id, description, ODS_region_id, date_created, last_update, record_orig_site_id)
	SELECT DISTINCT src_ter.TerritoryID,
	src_ter.TerritoryDescription,
	src_reg.ODS_region_id,
	getDate() AS date_created,
	NULL AS last_update,
	src_ter.SiteId
	FROM Stg_Territories src_ter
	LEFT JOIN region src_reg
	ON src_ter.RegionID = src_reg.orig_region_id
	LEFT JOIN territory tgt_ter
	ON src_ter.TerritoryID = tgt_ter.orig_territory_id
	WHERE tgt_ter.ODS_territory_id IS NULL
END


GO
/****************  
* PROCEDURE ETL_LoadEmployeeTerritory
* AUTHOR: bnowak
* DATE CREATED: 10/20/2024
*
* THIS PROCEDURE LOADS THE ODS EMPLOYEE TERRITORY TABLE FROM THE STG_EMPLOYEETERRITORIES TABLE
* IT INSERTS ONLY NEW RECORDS. IT DOES NOT DO ANY UPDATES OR DELETES
*
* CHANGE LOG
* --------------
*
****************/
ALTER PROCEDURE ETL_LoadEmployeeTerritory
AS
BEGIN
	INSERT INTO employee_territory(ODS_employee_id, ODS_territory_id, date_created)
	SELECT DISTINCT src_emp.ODS_employee_id,
	src_ter.ODS_territory_id,
	getDate() AS date_created
	FROM Stg_EmployeeTerritories src_et
	LEFT JOIN employee src_emp
	ON src_et.EmployeeID = src_emp.orig_employee_id
	LEFT JOIN territory src_ter
	ON src_et.TerritoryID = src_ter.orig_territory_id
	LEFT JOIN employee_territory tgt_et
	ON src_emp.ODS_employee_id = tgt_et.ODS_employee_id
	AND src_ter.ODS_territory_id = tgt_et.ODS_territory_id
	WHERE src_et.EmployeeID = src_emp.orig_employee_id
		AND src_emp.record_orig_site_id = 2
		AND tgt_et.ODS_employee_id IS NULL 
END


GO
/****************  
* PROCEDURE ETL_LoadProduct
* AUTHOR: bnowak
* DATE CREATED: 10/20/2024
*
* THIS PROCEDURE LOADS THE ODS PRODUCT TABLE FROM THE STG_STOCKITEM, STG_STOCKITEMINVENTORY, STG_ITEMCATEGORY, STG_PRODUCTS, AND STG_CATEGORIES TABLES
* IT INSERTS NEW RECORDS. IT ALSO UPDATES WHEN THERE IS A CHANGE TO A PRODUCT
*
* CHANGE LOG
* --------------
*
****************/
ALTER PROCEDURE ETL_LoadProduct
AS
BEGIN
	UPDATE product
	SET name = src_si.StockItemName,
	category_name = src_ic.Category,
	color = src_si.Color,
	size = src_si.Size,
	quantity_per_unit = CAST(src_si.QuantityPerOuter AS varchar(20)),
	unit_price = src_si.Cost,
	wholesale_price = src_si.RetailPrice,
	units_in_stock = src_sii.QuantityOnHand,
	reorder_level = src_sii.ReorderLevel,
	target_stock_level = src_sii.TargetStockLevel,
	bin_location = src_sii.BinLocation,
	discontinued = CAST(CASE WHEN src_si.IsActive = 1 THEN 0 ELSE 1 END AS bit),
	last_update = getDate()
	FROM Stg_StockItem src_si
	JOIN Stg_StockItemInventory src_sii
	ON src_si.StockItemId = src_sii.StockItemID
	JOIN Stg_ItemCategory src_ic
	ON src_si.ItemCategoryId = src_ic.CategoryId
	JOIN supplier src_sup
	ON src_si.SupplierId = src_sup.orig_supplier_id AND src_sup.site_id = 1
	JOIN product tgt_pro
	ON src_si.StockItemId = tgt_pro.orig_product_id
	WHERE NOT (ISNULL(src_si.StockItemName, '') = ISNULL(tgt_pro.name, '')
		AND ISNULL(src_ic.Category, '') = ISNULL(tgt_pro.category_name, '')
		AND ISNULL(src_si.Color, '') = ISNULL(tgt_pro.color, '')
		AND ISNULL(src_si.Size, '') = ISNULL(tgt_pro.size, '')
		AND CAST(ISNULL(src_si.QuantityPerOuter, '')AS varchar(20)) = ISNULL(tgt_pro.quantity_per_unit, '')
		AND ISNULL(src_si.Cost, -1) = ISNULL(tgt_pro.unit_price, -1)
		AND ISNULL(src_si.RetailPrice, -1) = ISNULL(tgt_pro.wholesale_price, -1)
		AND ISNULL(src_sii.QuantityOnHand, 0) = ISNULL(tgt_pro.units_in_stock, 0)
		AND ISNULL(src_sii.ReorderLevel, 0) = ISNULL(tgt_pro.reorder_level, 0)
		AND ISNULL(src_sii.TargetStockLevel, 0) = ISNULL(tgt_pro.target_stock_level, 0)
		AND ISNULL(src_sii.BinLocation, '') = ISNULL(tgt_pro.bin_location, '')
		AND CAST(CASE WHEN src_si.IsActive = 1 THEN 0 ELSE 1 END AS bit) = ISNULL(tgt_pro.discontinued, 0)
	)
	UPDATE product
	SET name = src_pro.ProductName,
	category_name = src_cat.CategoryName,
	category_description = src_cat.Description,
	quantity_per_unit = src_pro.QuantityPerUnit,
	unit_price = src_pro.UnitPrice,
	wholesale_price = src_pro.WholesalePrice,
	units_in_stock = src_pro.UnitsInStock,
	units_on_order = src_pro.UnitsOnOrder,
	reorder_level = src_pro.ReorderLevel,
	discontinued = src_pro.Discontinued,
	last_update = getDate()
	FROM Stg_Products src_pro
	JOIN Stg_Categories src_cat
	ON src_pro.CategoryID = src_cat.CategoryID
	JOIN supplier src_sup
	ON src_pro.SupplierID = src_sup.orig_supplier_id AND src_sup.site_id = 2
	JOIN product tgt_pro
	ON src_pro.ProductID = tgt_pro.orig_product_id
	WHERE NOT (ISNULL(src_pro.ProductName, '') = ISNULL(tgt_pro.name, '')
		AND ISNULL(src_cat.CategoryName, '') = ISNULL(tgt_pro.category_name, '')
		AND ISNULL(src_cat.Description, '') = ISNULL(tgt_pro.category_description, '')
		AND ISNULL(src_pro.QuantityPerUnit, '') = ISNULL(tgt_pro.quantity_per_unit, '')
		AND ISNULL(src_pro.UnitPrice, -1) = ISNULL(tgt_pro.unit_price, -1)
		AND ISNULL(src_pro.WholesalePrice, -1) = ISNULL(tgt_pro.wholesale_price, -1)
		AND ISNULL(src_pro.UnitsInStock, 0) = ISNULL(tgt_pro.units_in_stock, 0)
		AND ISNULL(src_pro.UnitsOnOrder, 0) = ISNULL(tgt_pro.units_on_order, 0)
		AND ISNULL(src_pro.ReorderLevel, 0) = ISNULL(tgt_pro.reorder_level, 0)
		AND ISNULL(src_pro.Discontinued, 0) = ISNULL(tgt_pro.discontinued, 0)
	)

	INSERT INTO product(orig_product_id, name, category_name, category_description, ODS_supplier_id, color, size, quantity_per_unit, unit_price, wholesale_price, units_in_stock, units_on_order, reorder_level, target_stock_level, bin_location, discontinued, date_created, last_update, site_id)
	SELECT DISTINCT src_si.StockItemId,
	src_si.StockItemName,
	src_ic.Category,
	NULL AS category_description,
	src_sup.ODS_supplier_id,
	src_si.Color,
	src_si.Size,
	CAST(src_si.QuantityPerOuter AS varchar(20)) AS quantity_per_unit,
	src_si.Cost,
	src_si.RetailPrice,
	src_sii.QuantityOnHand,
	NULL AS units_on_order,
	src_sii.ReorderLevel,
	src_sii.TargetStockLevel,
	src_sii.BinLocation,
	CAST(CASE
		WHEN src_si.IsActive = 1
		THEN 0 ELSE 1
	END AS bit) AS discontinued,
	getDate() AS date_created,
	src_sii.LastEditDateTime,
	src_si.SiteId
	FROM Stg_StockItem src_si
	LEFT JOIN Stg_StockItemInventory src_sii
	ON src_si.StockItemId = src_sii.StockItemID
	LEFT JOIN Stg_ItemCategory src_ic
	ON src_si.ItemCategoryId = src_ic.CategoryId
	LEFT JOIN supplier src_sup
	ON src_si.SupplierId = src_sup.orig_supplier_id
	LEFT JOIN product tgt_pro
	ON src_si.StockItemId = tgt_pro.orig_product_id
	WHERE src_sup.site_id = 1
		AND tgt_pro.ODS_product_id IS NULL
	UNION
	SELECT DISTINCT src_pro.ProductID,
	src_pro.ProductName,
	src_cat.CategoryName,
	src_cat.Description,
	src_sup.ODS_supplier_id,
	NULL AS color,
	NULL AS size,
	src_pro.QuantityPerUnit,
	src_pro.UnitPrice,
	src_pro.WholesalePrice,
	src_pro.UnitsInStock,
	src_pro.UnitsOnOrder,
	src_pro.ReorderLevel,
	NULL AS target_stock_level,
	NULL AS bin_location,
	src_pro.Discontinued,
	getDate() AS date_created,
	NULL AS last_update,
	src_pro.SiteId
	FROM Stg_Products src_pro
	LEFT JOIN Stg_Categories src_cat
	ON src_pro.CategoryID = src_cat.CategoryID
	LEFT JOIN supplier src_sup
	ON src_pro.SupplierID = src_sup.orig_supplier_id
	LEFT JOIN product tgt_pro
	ON src_pro.ProductID = tgt_pro.orig_product_id
	WHERE src_sup.site_id = 2
		AND tgt_pro.ODS_product_id IS NULL
END


GO
/****************  
* PROCEDURE ETL_LoadOrder
* AUTHOR: bnowak
* DATE CREATED: 10/20/2024
*
* THIS PROCEDURE LOADS THE ODS ORDER TABLE FROM THE STG_ORDERHEADER, STG_SHIPPERS, AND STG_ORDERS TABLES
* IT INSERTS NEW RECORDS. IT ALSO UPDATES WHEN THERE IS A CHANGE TO AN ORDER
*
* CHANGE LOG
* --------------
* 11/24 - corrected to populate addresses based on customer's address for site 1 AND fixed missing address data for customer 196
*
****************/
ALTER PROCEDURE ETL_LoadOrder
AS
BEGIN
	UPDATE "order"
	SET order_date = src_oh.OrderDate,
	ship_date = src_oh.ShipDate,
 	order_number = src_oh.CustomerPurchaseOrderNumber,
	last_update = getDate()
	FROM Stg_OrderHeader src_oh
	JOIN employee src_emp
	ON src_oh.SalesPersonId = src_emp.orig_employee_id AND src_emp.record_orig_site_id = 1
	JOIN "order" tgt_ord
	ON src_oh.OrderId = tgt_ord.orig_order_id
	WHERE NOT (ISNULL(src_oh.OrderDate, '') = ISNULL(tgt_ord.order_date, '')
		AND ISNULL(src_oh.ShipDate, '') = ISNULL(tgt_ord.ship_date, '')
		AND ISNULL(src_oh.CustomerPurchaseOrderNumber, '') = ISNULL(tgt_ord.order_number, '')
	)
	UPDATE "order"
	SET order_date = src_ord.OrderDate,
	required_date = src_ord.RequiredDate,
	ship_via = src_shi.CompanyName,
	shipping_company_phone = src_shi.Phone,
	freight = src_ord.Freight,
	last_update = getDate()
	FROM Stg_Orders src_ord
	JOIN Stg_Shippers src_shi
	ON src_ord.ShipVia = src_shi.ShipperID
	JOIN employee src_emp
	ON src_ord.EmployeeID = src_emp.orig_employee_id
	JOIN address src_add
	ON src_ord.ShipAddress = src_add.full_address AND src_ord.ShipCountry = src_add.country
	JOIN "order" tgt_ord
	ON src_ord.OrderID = tgt_ord.orig_order_id
	WHERE NOT (ISNULL(src_ord.OrderDate, '') = ISNULL(tgt_ord.order_date, '')
		AND ISNULL(src_ord.RequiredDate, '') = ISNULL(tgt_ord.required_date, '')
		AND ISNULL(src_shi.CompanyName, '') = ISNULL(tgt_ord.ship_via, '')
		AND ISNULL(src_shi.Phone, '') = ISNULL(tgt_ord.shipping_company_phone, '')
		AND ISNULL(src_ord.Freight, -1) = ISNULL(tgt_ord.freight, -1)
	)

	INSERT INTO "order"(orig_order_id, ODS_customer_id, ODS_employee_id, ODS_address_id, order_date, ship_date, required_date, order_number, ship_via, shipping_company_phone, freight, date_created, last_update, site_id)
	SELECT DISTINCT src_oh.OrderId,
	src_cus.ODS_customer_id,
	src_emp.ODS_employee_id,
	src_add.ODS_address_id,
	src_oh.OrderDate,
	src_oh.ShipDate,
	NULL AS required_date,
	src_oh.CustomerPurchaseOrderNumber,
	NULL AS ship_via,
	NULL AS shipping_company_phone,
	NULL AS freight,
	getDate() AS date_created,
	NULL AS last_update,
	src_oh.SiteId
	FROM Stg_OrderHeader src_oh
	LEFT JOIN customer src_cus
	ON CAST(src_oh.CustomerId AS varchar(5)) = src_cus.orig_customer_id
	LEFT JOIN employee src_emp
	ON src_oh.SalesPersonId = src_emp.orig_employee_id
	LEFT JOIN Stg_Customer src_ocus
	ON src_oh.CustomerId = src_ocus.CustomerId
	LEFT JOIN Stg_Address src_oadd
	ON src_ocus.DeliveryAddressId = src_oadd.AddressId
	LEFT JOIN address src_add
	ON src_oadd.AddressId = src_add.orig_address_id
	LEFT JOIN "order" tgt_ord
	ON src_oh.OrderId = tgt_ord.orig_order_id
	WHERE src_emp.record_orig_site_id = 1
		AND tgt_ord.ODS_order_id IS NULL
	UNION 
	SELECT DISTINCT src_ord.OrderID,
	src_cus.ODS_customer_id,
	src_emp.ODS_employee_id,
	src_add.ODS_address_id,
	src_ord.OrderDate,
	NULL AS ship_date,
	src_ord.RequiredDate,
	NULL AS order_number,
	src_shi.CompanyName,
	src_shi.Phone,
	src_ord.Freight,
	getDate() AS date_created,
	NULL AS last_update,
	src_ord.SiteId
	FROM Stg_Orders src_ord
	LEFT JOIN Stg_Shippers src_shi
	ON src_ord.ShipVia = src_shi.ShipperID
	LEFT JOIN customer src_cus
	ON src_ord.CustomerID = src_cus.orig_customer_id
	LEFT JOIN employee src_emp
	ON src_ord.EmployeeID = src_emp.orig_employee_id
	LEFT JOIN address src_add
	ON src_ord.ShipAddress = src_add.full_address AND src_ord.ShipCountry = src_add.country
	LEFT JOIN "order" tgt_ord
	ON src_ord.OrderID = tgt_ord.orig_order_id
	WHERE src_emp.record_orig_site_id = 2
		AND tgt_ord.ODS_order_id IS NULL
END


GO
/****************  
* PROCEDURE ETL_LoadOrderDetail
* AUTHOR: bnowak
* DATE CREATED: 10/20/2024
*
* THIS PROCEDURE LOADS THE ODS ORDER DETAIL TABLE FROM THE STG_ORDERLINE, AND STG_ORDERDETAILS TABLES
* IT INSERTS NEW RECORDS. IT ALSO UPDATES WHEN THERE IS A CHANGE TO AN ORDERS DETAILS
*
* CHANGE LOG
* --------------
*
****************/
ALTER PROCEDURE ETL_LoadOrderDetail
AS
BEGIN
	UPDATE order_detail
	SET quantity_per_unit = src_ol.Quantity,
	unit_price = src_ol.UnitPrice,
	tax_rate = src_ol.TaxRate,
	item_picked_date = src_ol.ItemPickedDate,
	last_update = getDate()
	FROM Stg_OrderLine src_ol
	JOIN "order" src_ord
	ON src_ol.OrderId = src_ord.orig_order_id AND src_ol.SiteId = src_ord.site_id
	JOIN product src_pro
	ON src_ol.StockItemId = src_pro.orig_product_id AND src_ol.SiteId = src_pro.site_id
	JOIN order_detail tgt_od
	ON src_ol.OrderLineId = tgt_od.orig_order_detail_id
	WHERE NOT (ISNULL(src_ol.Quantity, 0) = ISNULL(tgt_od.quantity_per_unit, 0)
		AND ISNULL(src_ol.UnitPrice, -1) = ISNULL(tgt_od.unit_price, -1)
		AND ISNULL(src_ol.TaxRate, 0) = ISNULL(tgt_od.tax_rate, 0)
		AND ISNULL(src_ol.ItemPickedDate, '') = ISNULL(tgt_od.item_picked_date, '')
	)
	UPDATE order_detail
	SET quantity_per_unit = src_od.Quantity,
	unit_price = src_od.UnitPrice,
	discount = src_od.Discount,
	last_update = getDate()
	FROM Stg_OrderDetails src_od
	JOIN "order" src_ord
	ON src_od.OrderId = src_ord.orig_order_id AND src_od.SiteId = src_ord.site_id
	JOIN product src_pro
	ON src_od.ProductID = src_pro.orig_product_id AND src_od.SiteId = src_pro.site_id
	JOIN order_detail tgt_od
	ON src_od.OrderID = tgt_od.orig_order_detail_id
	WHERE NOT (ISNULL(src_od.Quantity, 0) = ISNULL(tgt_od.quantity_per_unit, 0)
		AND ISNULL(src_od.UnitPrice, -1) = ISNULL(tgt_od.unit_price, -1)
		AND ISNULL(src_od.Discount, 0) = ISNULL(tgt_od.discount, 0)
	)

	INSERT INTO order_detail(orig_order_detail_id, ODS_order_id, ODS_product_id, quantity_per_unit, unit_price, tax_rate, item_picked_date, discount, date_created, last_update, site_id)
	SELECT DISTINCT src_ol.OrderLineId,
	src_ord.ODS_order_id,
	src_pro.ODS_product_id,
	src_ol.Quantity,
	src_ol.UnitPrice,
	src_ol.TaxRate,
	src_ol.ItemPickedDate,
	NULL AS discount,
	getDate() AS date_created,
	NULL AS last_update,
	src_ol.SiteId
	FROM Stg_OrderLine src_ol
	LEFT JOIN "order" src_ord
	ON src_ol.OrderId = src_ord.orig_order_id AND src_ol.SiteId = src_ord.site_id
	LEFT JOIN product src_pro
	ON src_ol.StockItemId = src_pro.orig_product_id AND src_ol.SiteId = src_pro.site_id
	LEFT JOIN order_detail tgt_od
	ON src_ol.OrderLineId = tgt_od.orig_order_detail_id
	WHERE tgt_od.ODS_order_detail_id IS NULL
	UNION
	SELECT DISTINCT src_od.OrderID,
	src_ord.ODS_order_id,
	src_pro.ODS_product_id,
	src_od.Quantity,
	src_od.UnitPrice,
	NULL AS tax_rate,
	NULL AS item_picked_date,
	src_od.Discount,
	getDate() AS date_created,
	NULL AS last_update,
	src_od.SiteId
	FROM Stg_OrderDetails src_od
	LEFT JOIN "order" src_ord
	ON src_od.OrderId = src_ord.orig_order_id AND src_od.SiteId = src_ord.site_id
	LEFT JOIN product src_pro
	ON src_od.ProductID = src_pro.orig_product_id AND src_od.SiteId = src_pro.site_id
	LEFT JOIN order_detail tgt_od
	ON src_od.OrderID = tgt_od.orig_order_detail_id
	WHERE tgt_od.ODS_order_detail_id IS NULL
END


GO
/****************  
* PROCEDURE ETL_ETLControl
* AUTHOR: bnowak
* DATE CREATED: 10/20/2024
*
* THIS PROCEDURE KICKS OFF ALL THE ETL PROCS IN THE CORRECT ORDER
*
* CHANGE LOG
* --------------
*
****************/
ALTER PROCEDURE ETL_ETLControl
AS
BEGIN
	EXEC ETL_LoadSite
	EXEC ETL_LoadRegion
	EXEC ETL_LoadAddress
	EXEC ETL_LoadCustomer
	EXEC ETL_LoadEmployee
	EXEC ETL_LoadSupplier
	EXEC ETL_LoadTerritory
	EXEC ETL_LoadEmployeeTerritory
	EXEC ETL_LoadProduct
	EXEC ETL_LoadOrder
	EXEC ETL_LoadOrderDetail
END


GO
	EXEC ETL_ETLControl