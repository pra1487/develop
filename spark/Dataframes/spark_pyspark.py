# Define functions in this file which hold Spark logic to generate the QVD datasets
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window
from functools import reduce
from pyspark.sql.types import ArrayType, IntegerType, DoubleType
import pytz
from datetime import datetime as dt

from data_asset.datasets.ontology.entities.helpers.product import (
    get_direct_sales_product,
    get_distributor_sales_products,
    get_harmonized_product_hierarchy_fdh,
    get_hpqs_products,
    get_ims_product,
    get_indian_veeva_products,
    get_jda_products,
    get_pricing,
    get_product_master_fdh,
    get_reporting_product,
    sap_pricentirc_pdts,
)
from ..utils._utils import (
    add_division,
    get_gps_details,
    get_record_type_details,
    add_in_stakeholder_id,
    clean_answer_choices,
    unicode_normalize,
    add_cdc_cols,
    standardize_country_code,
    long_to_date,
    union_datasets,
    union_different_df,
    rename_columns,
    empty_arrays_to_null,
    hash_into_key
)
from baseline.core.utils import safe_union
from ..utils.oneview_filters import one_view_filters
from ..gps.gps_division import gps_division

import re
import json


def calendar(**kwargs):
    # pre defined tertial based on Month
    Tertial_T1 = ['01', '02', '03', '04', '1', '2', '3', '4']
    Tertial_T2 = ['05', '06', '07', '08', '5', '6', '7', '8']
    date = kwargs['date']
    date = date.withColumn("Year_Tertial", F.when(F.col('month').isin(Tertial_T1), F.concat(F.col('year'),
                                                                                            F.lit('T1'))).when(
        F.col('month').isin(Tertial_T2),
        F.concat(F.col('year'), F.lit('T2'))).otherwise(F.concat(F.col('year'), F.lit('T3'))))
    date = date.withColumnRenamed("date", "date_unix_timestamp")
    date = date.withColumn('date', long_to_date(F.col('date_unix_timestamp')))

    return date


def country(**kwargs):
    country = kwargs['country']
    google = kwargs['google_analytics_countries']

    COLUMNS_SELECTED_COUNTRY = [
        'iso',
        'country_id',
        'iso_numeric',
        'fips',
        'latitude',
        'longitude',
        'country_name',
        'capital',
        'area_in_sq_km',
        'population',
        'continent',
        'top_level_domain',
        'currency',
        'currency_name',
        'phone',
        'postal_code_format',
        'postal_code_regex',
        'languages',
        'neighbours',
        'equivalent_fips_code',
        'area',
        'area_name',
        'region',
        'country_group_code',
        'country_group_name',
        'performance_code',
        'veeva_id',
        'veeva_country_code',
        'veeva_instance',
        'language_iso_639',
        'geohash',
        'affiliate',
        'affiliate_group',
        'country_group',
        'working_days'
    ]

    COLUMNS_SELECTED_AFFILIATES = [
        'weekday_sun',
        'weekday_mon',
        'weekday_tue',
        'weekday_wed',
        'weekday_thu',
        'weekday_fri',
        'weekday_sat',
    ]

    COLUMNS_AS_ARRAY_AFFILIATES = [
        'weekday_sun',
        'weekday_mon',
        'weekday_tue',
        'weekday_wed',
        'weekday_thu',
        'weekday_fri',
        'weekday_sat',
    ]

    COLUMNS_SELECTED_GOOGLE = [
        'country',
        'name',
    ]

    # use Null instead of 'nan' for Null values:
    for column in COLUMNS_SELECTED_AFFILIATES:
        country = country.withColumn(column,
                                     F.when(
                                         (F.lower(F.col(column)) == F.lit('nan')),
                                         F.lit(None)
                                     ).otherwise(
                                         F.col(column)
                                     )
                                     )
    # convert working days columns into one array column:
    country = country.withColumn('working_days', F.array())
    for column in COLUMNS_AS_ARRAY_AFFILIATES:
        country = country.withColumn('working_days',
                                     F.when(
                                         (F.lower(F.col(column)) == F.lit('x')),
                                         F.array_union(F.col('working_days'),
                                                       F.array(F.lit(str(COLUMNS_AS_ARRAY_AFFILIATES.index(column)))))
                                     ).otherwise(
                                         F.col('working_days')
                                     )
                                     ).drop(column)
        COLUMNS_SELECTED_AFFILIATES.remove(column)
    COLUMNS_SELECTED_AFFILIATES.append('working_days')

    # Prepare google dataset:
    google = google \
        .select(COLUMNS_SELECTED_GOOGLE) \
        .withColumnRenamed('country', 'iso2') \
        .withColumnRenamed('name', 'google_name')

    # Join left:
    country = country \
        .select(COLUMNS_SELECTED_COUNTRY) \
        .withColumnRenamed('iso', 'iso2') \
        .withColumn('mapbox_geoid', F.col('iso2')) \
        .withColumn('area_id', F.lower(F.regexp_replace('area', r"[&|\s]+", ""))) \
        .join(google, 'iso2', 'left')

    return country


def iso_country_mapping(**kwargs):
    country = kwargs['country']
    # Data quality filters
    country = country.where(
        (country.country_name != "N/A")
    )

    return country.select(
        country.iso2.alias("ISO2"),
        country.country_id.alias('ISO3'),
        country.google_name,
        country.country_name
    )


def area(**kwargs):
    country = kwargs['country']
    return (
        country.filter(F.lower(F.col('area')).contains('not defined') == False) \
            .select("area", "area_name", "area_id") \
            .distinct()
    )


def affiliate(**kwargs):
    country = kwargs['country']
    return (
        country.filter(F.col('affiliate').isNotNull()) \
            .select("affiliate") \
            .distinct()
    )


def business_unit(**kwargs):
    reporting_product = kwargs['reporting_product']
    COLUMNS_TO_BE_RENAMED = [
        ("business_unit", "business_unit")
    ]
    COLUMNS_TO_BE_SELECTED = [col[0] for col in COLUMNS_TO_BE_RENAMED]
    df = reporting_product.select(COLUMNS_TO_BE_SELECTED). \
        filter(reporting_product.business_unit != 'n/a'). \
        filter(reporting_product.business_unit != 'N/A')
    for columns in COLUMNS_TO_BE_RENAMED:
        df = df.withColumnRenamed(columns[0], columns[1])
    return df.distinct()


def brand(**kwargs):
    reporting_product = kwargs['reporting_product']
    pricing = kwargs['pricing_records']
    rheumatology = kwargs['rheumatology_projected']
    gastroenterology = kwargs['gastroenterology_projected']
    dermatology = kwargs['dermatology_projected']
    column_select = [
        'brand',
        'therapeutic_area',
        'company',
    ]

    # Columns to be selected from pricing
    COLUMNS_TO_BE_RENAMED_PRICING_RECORDS = [
        ("product", "brand"),
        ("therapeutical_area", "therapeutic_area"),
        ("branded_type", 'branded_type'),
        ("company", "company")
    ]

    COLUMNS_TO_BE_SELECTED_PRICING_RECORDS = [col[1] for col in COLUMNS_TO_BE_RENAMED_PRICING_RECORDS]
    # Direct Sales Target is HQPS allergan data
    # replace GROUP, TOTAL key words , as they dont represent brand
    reporting_product = reporting_product \
        .withColumn('company',
                    F.when(F.col('source') == 'Direct Sales Target', F.lit('Allergan')).otherwise(F.col('company'))) \
        .withColumn('brand', F.when(
        (F.col('source') == 'Direct Sales Target') & (F.upper('brand').like('%GROUP%')),
        F.trim(F.upper(F.regexp_replace(F.upper('brand'), 'GROUP', '')))
    ).otherwise(F.upper('brand'))) \
        .withColumn('brand', F.when(
        (F.col('source') == 'Direct Sales Target') & (F.upper('brand').like('%TOTAL%')),
        F.trim(F.upper(F.regexp_replace(F.upper('brand'), 'TOTAL', '')))
    ).otherwise(F.upper('brand')))
    output = reporting_product.select(column_select) \
        .filter(
        (F.col('brand') != 'ABBVIE') &
        (F.col('brand') != 'n/a') &
        (F.col('brand') != 'N/A') &
        (F.col('brand').contains('TOTAL') == False) &
        (F.col('brand').isNotNull())
    ) \
        .drop_duplicates()
    # identify the branded type for the vailable brand
    pricing_branded_types = pricing.select(F.col('product').alias('brand'), 'branded_type')

    brand = output.join(pricing_branded_types, 'brand', 'left').drop_duplicates()

    # Merge the brands from pricing
    # ---------------- Organizations from PRICENTRIC ------------------- #
    # Renaming columns in order to be merged
    for columns in COLUMNS_TO_BE_RENAMED_PRICING_RECORDS:
        pricing = pricing.withColumnRenamed(columns[0], columns[1])

    # Select only a subset of columns
    pricentric_brands = pricing.select(
        *COLUMNS_TO_BE_SELECTED_PRICING_RECORDS).drop_duplicates()

    pricentric_brands = pricentric_brands. \
        join(
        brand.select('brand'),
        'brand',
        'left_anti'
    ).drop_duplicates()

    # All brands Reporting Product + pricentric_brands
    brand = brand.unionByName(pricentric_brands)
    # to resolve primary key issue converted the therapeutic_area column to array type ends

    dermatology = dermatology.select('drug'). \
        withColumn('brand', F.upper(F.split('drug', ' ')[0])). \
        withColumn('therapeutic_area', F.lit('DERMATOLOGY')). \
        withColumn('branded_type', F.when(F.col('drug').like('%bios%'), F.lit('BIOSIMILAR'))). \
        select('brand', 'therapeutic_area', 'branded_type')
    rheumatology = rheumatology.select('drug'). \
        withColumn('brand', F.upper(F.split('drug', ' ')[0])). \
        withColumn('therapeutic_area', F.lit('Rheumatology')). \
        withColumn('branded_type', F.when(F.col('drug').like('%bios%'), F.lit('BIOSIMILAR'))). \
        select('brand', 'therapeutic_area', 'branded_type')
    gastroenterology = gastroenterology.select('drug'). \
        withColumn('brand', F.upper(F.split('drug', ' ')[0])). \
        withColumn('therapeutic_area', F.lit('GASTROENTEROLOGY')). \
        withColumn('branded_type', F.when(F.col('drug').like('%bios%'), F.lit('BIOSIMILAR'))). \
        select('brand', 'therapeutic_area', 'branded_type')
    brands_from_ipsos = gastroenterology.unionByName(dermatology)
    brands_from_ipsos = brands_from_ipsos.unionByName(rheumatology)
    brands_from_ipsos = brands_from_ipsos.withColumn('company', F.lit(None))
    brand = brand.unionByName(brands_from_ipsos)
    # to resolve primary key issue converted the therapeutic_area column to array type starts
    brand = brand.groupBy('brand'). \
        agg(
        F.collect_set(F.upper(F.col('therapeutic_area'))).alias('therapeutic_area'),
        F.collect_set(F.col('company')).alias('company'),
        F.collect_set(F.col('branded_type')).alias('branded_type'))

    return brand


def working_day_calendar(**kwargs):
    holidays = kwargs['holidaylist']
    country = kwargs['country']
    """
    This is basically a date dimension dataset.
    For each country it also indicates if the date is a weekday, weekend, or holiday
    """

    COLUMNS_TO_BE_RENAMED_HOLIDAYS = [
        ("activity_date", "h_date"),
        ("country_id", "h_country")
    ]

    COLUMNS_TO_BE_SELECTED_HOLIDAYS = [
        "h_date",
        "h_country"
    ]

    COLUMNS_TO_BE_SELECTED_COUNTRY = [
        'working_days',
        'iso2',
        'country_id',
        'affiliate'
    ]

    COLUMNS_TO_BE_SELECTED_OUTPUT = [
        'date',
        'country_id',
        'day_type',
        'affiliate'
    ]
    holidays = holidays.filter(F.col('source') == 'veeva').withColumn("activity_date",
                                                                      F.to_date("activity_date", "yyyyMMdd"))

    # Get all holidays in all countries
    for old_col, new_col in COLUMNS_TO_BE_RENAMED_HOLIDAYS:
        holidays = holidays.withColumnRenamed(old_col, new_col)

    holidays = (
        holidays
            .select(*COLUMNS_TO_BE_SELECTED_HOLIDAYS)
            .dropDuplicates()
    )

    # create all dates
    df = (
        holidays
            .agg(F.min('h_date').alias('min_date'), F.max('h_date').alias('max_date'))
            .withColumn('start', F.format_string('%d-01-01', F.year('min_date')).cast('date'))
            .withColumn('end', F.format_string('%d-12-31', F.year('max_date')).cast('date'))
            .withColumn('date', F.explode(F.expr('sequence(start, end, interval 1 day)')))
            .select('date')
            .withColumn('dayofweek', (F.dayofweek('date') - 1).cast(
            'string'))  # subtracting and casting to match with data in country
            .withColumn('day_type', F.lit('weekday'))  # marking all dates as weekday
    )

    # for all countries, get their working days
    all_countries = country.select(*COLUMNS_TO_BE_SELECTED_COUNTRY)

    # cross join with countries to populate all dates for all countries
    df = df.crossJoin(all_countries)

    # update weekends
    df = (
        df
            .withColumn('day_type',
                        F.when(
                            ~(F.expr("array_contains(working_days, dayofweek)")),
                            F.lit('weekend')
                        ).otherwise(df.day_type))
            .select('date', 'day_type', 'iso2', 'country_id', 'affiliate')
    )

    # update holidays
    df = df.join(holidays, F.expr("(date = h_date) and ((iso2 = h_country) or (country_id = h_country))"), how='left')
    df = (
        df
            .withColumn('day_type', F.when(F.col('h_date').isNotNull(), F.lit('holiday')).otherwise(df.day_type))
            .select(*COLUMNS_TO_BE_SELECTED_OUTPUT)
    )

    return df


def indication(**kwargs):
    reporting_product_sales = kwargs['reporting_product_sales']
    veeva_product = kwargs['veeva_product']
    pricentric = kwargs['pricentric']
    # Column editing
    reporting_product_sales = reporting_product_sales.select('indication')
    veeva_product = veeva_product.select('indication')
    pricentric = pricentric.select('indication')

    # union all therapeutic_area rp + veeva + pricentric
    indication = safe_union(reporting_product_sales, veeva_product)
    indication = safe_union(indication, pricentric)

    indication = indication.filter(F.col("indication").isNotNull()).drop_duplicates()

    return indication


def client_device(**kwargs):
    approved_email = kwargs['approved_email']
    COLUMNS_TO_BE_RENAMED_APPROVED_EMAIL = [
        ('email_activity_client_name', 'client_name'),
        ('email_activity_client_os', 'client_os'),
        ('email_activity_client_type', 'client_type'),
        ('email_activity_device_type', 'device_type'),
        ('row_is_current', 'row_is_current'),
        ('row_last_modified', 'row_last_modified')
    ]

    COLUMNS_TO_BE_SELECTED_APPROVED_EMAIL = [col[1] for col in COLUMNS_TO_BE_RENAMED_APPROVED_EMAIL]

    # records deleted from veeva are not considered
    approved_email = approved_email.filter(F.col('is_deleted') == False)
    # Renaming columns in order to be merged
    for columns in COLUMNS_TO_BE_RENAMED_APPROVED_EMAIL:
        approved_email = approved_email.withColumnRenamed(columns[0], columns[1])

    # Select only needed columns
    client_device = approved_email.select(*COLUMNS_TO_BE_SELECTED_APPROVED_EMAIL).distinct()
    client_device = client_device.withColumn('channel_source', F.lit('Approved E-mail'))

    client_device = client_device. \
        withColumn('client_name', F.when(
        F.col('client_name') == 'unknown', F.lit(None)).otherwise(F.col('client_name'))
                   ).withColumn('client_os', F.when(
        F.col('client_os') == 'unknown', F.lit(None)).otherwise(F.col('client_os'))
                                ).withColumn('client_type', F.when(
        F.col('client_type') == 'unknown', F.lit(None)).otherwise(F.col('client_type'))
                                             ).withColumn('device_type', F.when(
        F.col('device_type') == 'unknown', F.lit(None)).otherwise(F.col('device_type'))
                                                          )
    client_device = client_device.filter(
        F.col('client_name').isNotNull() & F.col('client_os').isNotNull() &
        F.col('client_type').isNotNull() & F.col('device_type').isNotNull())

    client_device = client_device.withColumn('login_details',
                                             F.concat(F.col('device_type'), F.lit(' '), F.col('client_type'),
                                                      F.lit(" : "), F.col('client_name'), F.lit(" on "),
                                                      F.col('client_os')))

    client_device = client_device.withColumn('client_device_id', F.sha2(
        F.concat_ws("_", *['client_name', 'client_os', 'client_type', 'device_type']), 256))
    client_device = client_device.filter(
        (F.col('client_device_id').isNotNull()) | (F.length(F.col('client_device_id')) > 0))
    client_device = add_cdc_cols(client_device, 'client_device')
    return client_device.distinct()


def contact_reason(**kwargs):
    veeva_corrections = kwargs['veeva_corrections']
    veeva_products = kwargs['veeva_products']
    record_type = kwargs['record_type']
    reporting_product = kwargs['reporting_product']
    iso_country_mapping = kwargs['iso_country_mapping']
    country = kwargs['country']
    call_details = kwargs['calls_details']
    segmentation = kwargs['segmentation']
    oneview_contact_reason = kwargs['oneview_contact_reason']

    COLUMNS_TO_BE_RENAMED_VEEVA_CORRECTIONS = [
        ("product_id", "contact_reason_id"),
        ("indication_therapeutic_area", "corrected_therapeutic_area"),
        ("indication_name", "corrected_indication_name"),
        ("product_brand", "brand"),
        ("product_company", "company"),
        ("role_function", "function"),
        ("role_role", "role"),
        ("product_molecules", "molecule")
    ]

    COLUMNS_TO_BE_SELECTED_VEEVA_CORRECTIONS = [col[1] for col in COLUMNS_TO_BE_RENAMED_VEEVA_CORRECTIONS]

    COLUMNS_TO_BE_RENAMED_VEEVA_PRODUCTS = [
        ("country", "country_id"),
        ("product_id", "contact_reason_id"),
        ("name", "contact_reason_name"),
        ("parent_product", "parent_contact_reason_id"),
        ("product_compound", "compound"),
        ("product_indication", "contact_indication"),
        ("product_share_type", "share_type"),
        ("product_type", "type"),
        ("is_company_product", "abbvie_flag"),
        ("med_flg_abb__c", "is_medical")
    ]

    COLUMNS_TO_BE_SELECTED_VEEVA_PRODUCTS = [
        "country_id",
        "contact_reason_id",
        "contact_reason_name",
        "create_date",
        "created_by_id",
        "description",
        "indication",
        "abbvie_flag",
        "is_controlled_substance",
        "item_code",
        "last_modified_date",
        "manufacturer",
        "owner_id",
        "parent_contact_reason_id",
        "parent_provider_id",
        "compound",
        "contact_indication",
        "share_type",
        "type",
        "provider_id",
        "record_type_id",
        "row_is_current",
        "therapeutic_area",
        "therapeutic_class",
        "is_medical",
        "row_last_modified",
        "segment"
    ]

    COLUMNS_TO_BE_RENAMED_RECORD_TYPE = [
        ("id", "record_type_id"),
        ("name", "record_name"),
        ("description", "record_description")
    ]

    COLUMNS_TO_BE_SELECTED_RECORD_TYPE = [col[1] for col in COLUMNS_TO_BE_RENAMED_RECORD_TYPE]

    COLUMNS_TO_BE_RENAMED_BRAND = [
        ('brand', 'rp_brand'),
        ('veeva_product_id', 'veeva_product_id'),
        ('business_unit', "business_unit"),
        ('rp_therapeutic_area', 'rp_therapeutic_area'),
        ('rp_indication', 'rp_indication'),
        ('ontology_therapeutic_area', 'therapeutic_area_medical')
    ]

    COLUMNS_TO_BE_SELECTED_BRAND = [col[1] for col in COLUMNS_TO_BE_RENAMED_BRAND]

    # --------------------- CORRECTIONS ---------------------- #

    # Renaming columns in order to be merged
    for columns in COLUMNS_TO_BE_RENAMED_VEEVA_CORRECTIONS:
        veeva_corrections = veeva_corrections.withColumnRenamed(columns[0], columns[1])

    # Select only a subset of columns
    veeva_corrections = veeva_corrections.select(
        *COLUMNS_TO_BE_SELECTED_VEEVA_CORRECTIONS)

    # Make sure there are no duplicates
    veeva_corrections = veeva_corrections.dropDuplicates()

    # ------------------- RECORD TYPE ----------------------- #
    # Renaming columns in order to be merged
    for columns in COLUMNS_TO_BE_RENAMED_RECORD_TYPE:
        record_type = record_type.withColumnRenamed(columns[0], columns[1])

    # Select only a subset of columns
    record_type = record_type.select(
        *COLUMNS_TO_BE_SELECTED_RECORD_TYPE)

    # Make sure there are no duplicates
    record_type = record_type.dropDuplicates()

    # ---------------------- PRODUCT ------------------------ #

    # Renaming columns
    for columns in COLUMNS_TO_BE_RENAMED_VEEVA_PRODUCTS:
        veeva_products = veeva_products.withColumnRenamed(columns[0], columns[1])

    # Select only a subset of columns
    veeva_products = veeva_products.select(
        *COLUMNS_TO_BE_SELECTED_VEEVA_PRODUCTS)

    # Filter on only the current entries
    veeva_products = veeva_products \
        .filter(F.col("row_is_current") == True)

    # Convert country string list into array to explode later on
    veeva_products = veeva_products \
        .withColumn(
        "country_id",
        F.split("country_id", ',')
    ).withColumn(
        "country_id",
        F.explode("country_id")
    ).withColumn(
        "sub_therapeutic_area",
        F.col("therapeutic_area")
    )

    # Note - we should insert the join with the my setup products here to limit
    # the products-country associations to only those that have actually set them up, vs.
    # few country is lower case , ex: Gl
    veeva_products = veeva_products.withColumn('country_id', F.upper(F.col('country_id')))
    # Standardize country code
    veeva_products = standardize_country_code(veeva_products, iso_country_mapping)
    veeva_products = veeva_products.dropDuplicates()
    # find products that are available for other countries , but those country not available in PRODUCT METRIC
    # eg a006F00002d9MfSQAU available for BRA,DZA,POL in clean/products , but there is a call for CHN
    orphan_products = identify_orphan_products(call_details, veeva_products, country)
    veeva_products = veeva_products.unionByName(orphan_products)
    orphan_products = identify_orphan_products(segmentation, veeva_products, country)
    veeva_products = veeva_products.unionByName(orphan_products)

    # ------------- ENRICH VEEVA PRODUCT --------------------- #

    # Join record type onto veeva product
    contact_reason = veeva_products \
        .join(
        record_type,
        on='record_type_id',
        how='left'
    )

    # Join corrections onto Product
    contact_reason = contact_reason \
        .join(
        veeva_corrections,
        on='contact_reason_id',
        how='left'
    )

    # Standardise the contact reason ID column via addition of countries
    contact_reason = contact_reason \
        .withColumn(
        "contact_reason_id",
        F.concat_ws(
            '_',
            *[F.col("country_id"),
              F.col("contact_reason_id")]
        )
    ).withColumn(
        "parent_contact_reason_id",
        F.when(F.col('parent_contact_reason_id').isNotNull(), F.concat_ws(
            '_',
            *[F.col("country_id"),
              F.col("parent_contact_reason_id")]
        )).otherwise(F.lit(None))
    )
    reporting_product = reporting_product_brand(reporting_product)

    # Renaming columns in order to be merged
    for columns in COLUMNS_TO_BE_RENAMED_BRAND:
        reporting_product = reporting_product.withColumnRenamed(columns[0], columns[1])

    reporting_product = reporting_product.select(
        *COLUMNS_TO_BE_SELECTED_BRAND
    )
    # reporting_product = max_row(reporting_product, comparison_columns=["rp_brand", "therapeutic_area_medical"], key_columns=['veeva_product_id'])
    # Harmonise the brand column from reproting product
    contact_reason = contact_reason \
        .withColumn(
        'veeva_product_id', F.split('contact_reason_id', '_')[1]
    ) \
        .join(
        reporting_product,
        'veeva_product_id',
        'left'
    ) \
        .withColumn(
        'brand', F.upper(F.coalesce('rp_brand', 'brand', F.lit('not mapped in reporting_product')))
    ) \
        .drop(
        'veeva_product_id',
        'rp_brand'
    )
    # Harmonise the indication and therapeutic area columns
    contact_reason = contact_reason \
        .withColumn(
        "indication",
        F.coalesce(
            F.col("rp_indication"),
            F.col("corrected_indication_name"),
            F.col("indication"),
            F.col("contact_indication")
        )
    ).withColumn(
        "therapeutic_area",
        F.coalesce(
            F.col("rp_therapeutic_area"),
            F.col("corrected_therapeutic_area"),
            F.col("therapeutic_area")
        )
    ).drop(
        "corrected_indication_name",
        "product_indication",
        "corrected_therapeutic_area",
        'rp_therapeutic_area',
        'rp_indication'
    )
    contact_reason = contact_reason.withColumn("is_medical", F.when(
        ((F.col('country_id') == 'JPN') &
         ((F.col('provider_id').startswith('GLO_MED_')) | (F.col('provider_id').startswith('M')))), F.lit(True)
    ).otherwise(
        F.when(
            ((F.col('country_id') != 'JPN') &
             (
                     (F.col('share_type') == 'Medical') |
                     (F.col('share_type').isNull() & F.lower(F.col('provider_id')).contains('glo_med'))
             )), F.lit(True)
        ).otherwise(
            F.lit(False)
        )
    ))
    contact_reason = contact_reason.withColumn("therapeutic_area", F.when(
        (F.col('is_medical') == True), F.col('sub_therapeutic_area')
    ).otherwise(F.col("therapeutic_area"))
                                               )
    contact_reason = add_division(contact_reason)
    contact_reason = contact_reason.withColumn('share_type',
                                               F.when(F.col('country_id') == 'JPN', F.col('division')).otherwise(
                                                   F.col('share_type')))
    contact_reason = get_gps_details(contact_reason, country)
    # clean up
    contact_reason = contact_reason.filter(F.col('country_id') != F.col('contact_reason_id'))
    contact_reason = gps_division(contact_reason, 'contact_reason')
    # Germany uses external sheet to fill the contact reasons that should flow in oneview
    oneview_contact_reason = oneview_contact_reason.withColumn('is_not_allowed_in_oneview', F.lit(True))
    contact_reason = contact_reason.join(
        oneview_contact_reason.select('contact_reason_id', 'is_not_allowed_in_oneview'),
        'contact_reason_id',
        'left') \
        .withColumn('is_not_allowed_in_oneview', F.coalesce('is_not_allowed_in_oneview', F.lit(False)))
    contact_reason = one_view_filters(contact_reason, 'contact_reason')
    return contact_reason.filter(F.length(F.col('country_id')) == 3)


def identify_orphan_products(df, veeva_products, country):
    # find products that are available for other countries
    # eg a006F00002d9MfSQAU - BRA,DZA,POL , but there is a call for CHN
    df = df.filter(F.col('product_id').isNotNull()).select(F.col('country').alias('country_id'), 'product_id')
    df = df.withColumn('is_invalid', F.col("country_id").startswith('XX'))
    df = df.withColumn(
        'country_id', F.regexp_replace(F.col('country_id'), r'^[XX]*', '')
    ).withColumn(
        'iso2', F.col('country_id')
    )
    df_corrrect_country = df.filter(F.col('is_invalid'))
    df_corrrect_country = df_corrrect_country.drop('country_id').join(
        country.select('iso2', 'country_id').repartition(1), 'iso2', 'left')
    df = df.filter((F.col('is_invalid').isNull()) | (F.col('is_invalid') == False)).unionByName(
        df_corrrect_country).drop('is_invalid', 'iso2')
    orphan_products = df.join(
        veeva_products,
        ((df.product_id == veeva_products.contact_reason_id) & (df.country_id == veeva_products.country_id)),
        'left_anti'
    ).distinct()

    orphan_products = orphan_products.join(
        veeva_products.drop('country_id').distinct(),
        df.product_id == veeva_products.contact_reason_id,
        'left').drop('product_id')
    return orphan_products


def reporting_product_brand(reporting_product):
    """
    returns a mapping of veeva_product_id to brand name
    """
    # Columns renaming reporting product
    COLUMNS_TO_BE_RENAMED = [
        ("source_product_identifier", "veeva_product_id"),
        ("brand", "rp_brand"),
        ('business_unit', "business_unit"),
        ('therapeutic_area', 'rp_therapeutic_area'),
        ('indication', 'rp_indication'),
        ('ontology_therapeutic_area', 'therapeutic_area_medical')
    ]

    # Columns to be selected
    COLUMNS_TO_BE_SELECTED = [
        "{column}".format(column=columns[1]) for columns in COLUMNS_TO_BE_RENAMED
    ]
    # rename the columns
    for columns in COLUMNS_TO_BE_RENAMED:
        reporting_product = reporting_product.withColumnRenamed(columns[0], columns[1])

    # Products assigned Globally are considered.
    products = reporting_product.filter(
        (F.col('country_code') == 'Glo') & (F.col('source') == 'Activity')
    )
    products = products.select(*COLUMNS_TO_BE_SELECTED).distinct()
    return products


def location(**kwargs):
    address = kwargs['reltio_address']
    pricentric = kwargs['pricentric']
    iso_country_mapping = kwargs['iso_country_mapping']
    veeva_address = kwargs['veeva_address']
    country = kwargs['country']
    # Columns to be selected in address
    COLUMNS_TO_BE_RENAMED = [
        ("address_id", "location_id"),
        ("addressline_1", "addressline_1"),
        ("addressline_2", "addressline_2"),
        ("city", "city"),
        ("country_code", "country_id"),
        ("geoloclati_longi_geoacc", "geoloclati_longi_geoacc"),
        ("pobox", "pobox"),
        ("postalcity", "postalcity"),
        ("premise", "premise"),
        ("stateprovince", "state_province"),
        ("stateprovincecode", "state_province_code"),
        ("street", "street"),
        ("subadministrativearea", "subadministrative_area"),
        ("unit", "unit"),
        ("verificationstatus", "verification_status"),
        ("zipzip_5_zip_4", "zip_code"),
        ("avc", "avc"),
        ("bricktyp_brickval_sortord", "brick_code"),
        ("row_is_current", "row_is_current"),
        ("row_last_modified", "row_last_modified")
    ]

    COLUMNS_TO_BE_SELECTED = [
        "{column}".format(column=columns[1]) for columns in COLUMNS_TO_BE_RENAMED
    ]

    # Columns to be selected in pricentric
    COLUMNS_TO_BE_RENAMED_PRICENTRIC = [
        ("country_province", "location_id"),
        ("country", "country_id"),
        ("row_is_current", "row_is_current"),
        ("row_last_modified", "row_last_modified")
    ]

    COLUMNS_TO_BE_SELECTED_PRICENTRIC = [
        "{column}".format(column=columns[1]) for columns in COLUMNS_TO_BE_RENAMED_PRICENTRIC
    ]

    # Columns to be selected in address
    COLUMNS_TO_BE_RENAMED_VEEVA_ADDRESS = [
        ("location_id", "location_id"),
        ("address_line1", "addressline_1"),
        ("address_line2", "addressline_2"),
        ("city", "city"),
        ("country_id", "country_id"),
        ("postal_city", "postalcity"),
        ("latam_state_province", "state_province"),
        ("microbrick_id", "microbrick_id"),
        ("brick_name", "brick_name"),
        ("brick_code", "brick_code"),
        ("postal_code", "zip_code_veeva"),
        ("row_is_current", "row_is_current"),
        ("row_last_modified", "row_last_modified")
    ]

    COLUMNS_TO_BE_SELECTED_VEEVA_ADDRESS = [
        "{column}".format(column=columns[1]) for columns in COLUMNS_TO_BE_RENAMED_VEEVA_ADDRESS
    ]

    # rename the columns
    for columns in COLUMNS_TO_BE_RENAMED:
        address = address.withColumnRenamed(columns[0], columns[1])
    address = address.select(*COLUMNS_TO_BE_SELECTED).distinct()

    # standardize country
    address = standardize_country_code(
        address,
        iso_country_mapping
    )
    address = address.withColumn('source', F.lit('Reltio'))

    # ------------------------PRICENTRIC----------------------------#

    # rename the columns
    for columns in COLUMNS_TO_BE_RENAMED_PRICENTRIC:
        pricentric = pricentric.withColumnRenamed(columns[0], columns[1])

    # if no province , then filter those records
    pricentric = pricentric.filter(
        F.col('location_id') != 'N/A'
    )
    pricentric = pricentric.select(*COLUMNS_TO_BE_SELECTED_PRICENTRIC).distinct()

    pricentric = pricentric.withColumn('type', F.lit('province'))
    # standardize country
    pricentric = standardize_country_code(
        pricentric,
        iso_country_mapping
    )
    pricentric = pricentric.withColumn('source', F.lit('Pricentirc'))
    # veeva address
    veeva_address = veeva_address.withColumnRenamed(
        'country', 'country_id'
    )
    # standardize country
    veeva_address = standardize_country_code(
        veeva_address,
        iso_country_mapping
    )

    veeva_address = veeva_address.dropDuplicates(['address_line1', 'brick_code', 'brick_name',
                                                  'city', 'country_id', 'latam_state_province', 'microbrick_id',
                                                  'postal_code'])

    veeva_address = veeva_address.withColumn("location_id",
                                             F.concat_ws("_", 'address_line1', 'brick_code', 'brick_name',
                                                         'city', 'country_id',
                                                         'latam_state_province',
                                                         'microbrick_id', 'postal_code'))

    veeva_address = veeva_address.withColumn('location_id', F.sha2(F.col('location_id'), 256))

    # rename the columns
    for columns in COLUMNS_TO_BE_RENAMED_VEEVA_ADDRESS:
        veeva_address = veeva_address.withColumnRenamed(columns[0], columns[1])

    veeva_address = veeva_address.select(COLUMNS_TO_BE_SELECTED_VEEVA_ADDRESS) \
        .drop('state_province') \
        .dropDuplicates()

    veeva_address = veeva_address.withColumn('source', F.lit('Veeva'))
    address = safe_union(address, pricentric)
    address = safe_union(address, veeva_address)
    address = address.withColumn('zip_code', F.trim(F.regexp_replace(F.col('zip_code'), "[^A-Z0-9_]", '')))
    address = address.withColumn('zip_code', F.when(
        F.col('source') == 'Veeva', F.col('zip_code_veeva')). \
                                 otherwise(F.col('zip_code'))).drop('zip_code_veeva')
    # address = add_cdc_cols(address, 'location')
    # to remove duplicates on location_id
    update_window = Window.partitionBy('location_id').orderBy(address.zip_code.desc(), address.row_last_modified.desc())
    address = address.withColumn(
        "brick_code", F.collect_set('brick_code').over(update_window)[0]
    ).withColumn(
        "brick_name", F.collect_set('brick_name').over(update_window)[0]
    ).withColumn(
        "postalcity", F.collect_set('postalcity').over(update_window)[0]).dropDuplicates()
    address = address.withColumn(
        "row_number", F.row_number().over(update_window)
    )
    address = address.where(address.row_number == '1').drop('row_number')
    address = get_gps_details(address, country)
    return address


def therapeutic_area(**kwargs):
    veeva_products = kwargs['veeva_products']
    medical_insights = kwargs['medical_insights']
    pricentric = kwargs['pricentric']
    # Select only a required  column
    COLUMNS_TO_BE_SELECTED = [
        "therapeutic_area"
    ]

    # not a valid therapeutic_area - to be removed list
    CLEAN_UP_LIST = ['No TA']
    # ------------------------ therapeutic_area from veeva products ----------------------#
    veeva_products = veeva_products.withColumnRenamed(
        'indication_therapeutic_area', 'therapeutic_area'
    ).select(
        COLUMNS_TO_BE_SELECTED
    ).distinct()

    # ------------------------ therapeutic_area from medical_insights ----------------------#
    medical_insights = medical_insights.select(
        COLUMNS_TO_BE_SELECTED
    ).distinct()

    # --------------PRICENTRIC--------------------#
    pricentric = pricentric.withColumnRenamed('therapeutical_area', 'therapeutic_area'). \
        select(COLUMNS_TO_BE_SELECTED)

    # union all therapeutic_area medical_insights + veeva + pricentric
    therapeutic_area = veeva_products. \
        unionByName(
        medical_insights
    ). \
        unionByName(
        pricentric
    )

    # applying filter to provide clean data
    therapeutic_area = therapeutic_area.filter(
        F.col('therapeutic_area').isNotNull() &
        (~F.col('therapeutic_area').isin(CLEAN_UP_LIST))
    ).dropDuplicates()

    return therapeutic_area


def territory(**kwargs):
    territory = kwargs['territory']
    iso_country_mapping = kwargs['iso_country_mapping']
    country = kwargs['country']
    one_emp_many_territory = kwargs['one_employee_to_many_territories']
    employee = kwargs['employee']
    one_view_function = kwargs['one_view_function']
    one_view_salesforce = kwargs['one_view_salesforce']

    INPUT_TERRITORY_SELECT = [
        "name",
        "id",
        "businessunit",
        "salesforce",
        "territory_function",
        "country",
        "description",
        "id_parent",
        "row_is_current",
        "row_start_date",
        "row_end_date",
        "row_last_modified"
    ]

    SELECT_COLS = [
        "country_id",
        "veeva_parent_territory_id",
        "territory_id",
        "veeva_territory_id",
        "business_unit",
        "salesforce",
        "territory_function",
        "territory_description",
        "parent_territory_id",
        "row_start_date",
        "row_end_date",
        "row_is_current",
        "row_last_modified"
    ]

    SELECT_COLS_PARTITION = [
        "country_id",
        "veeva_parent_territory_id",
        "territory_id",
        "veeva_territory_id",
        "business_unit",
        "salesforce",
        "territory_function",
        "territory_description",
        "row_start_date",
        "row_end_date",
        "row_is_current",
        "row_last_modified"
    ]

    tl2_window = [
        "veeva_parent_territory_id_clean",
        "parent_territory_id"
    ]

    SELECT_FINAL_TERRITORY_COLS = [
        "country_id",
        "veeva_parent_territory_id",
        "territory_id",
        "veeva_territory_id",
        "employee_business_unit",
        "salesforce",
        "employee_territory_function",
        "territory_description",
        "parent_territory_id",
        "row_start_date",
        "row_end_date",
        "row_is_current",
        "affiliate",
        "business_unit",
        "territory_function",
        "row_last_modified",
        "veeva_instance"
    ]

    # territory deduplication (temperory fix) - starts :
    # drop duplicates
    territory = territory.dropDuplicates()
    territory_accumulation = territory.filter(F.col('row_source').like("%new_df%"))
    territory_hist = territory.filter(
        (F.col('row_source').like("%history_df%")) | (F.col('row_source').like("%manual%")))
    main_cols = territory_accumulation.columns
    main_cols.remove('row_end_date')
    main_cols.remove('row_hash')
    main_cols.remove('row_is_current')
    main_cols.remove('row_operations')
    main_cols.remove('row_source')
    main_cols.remove('row_start_date')
    main_cols.remove('system_mod_stamp')
    main_cols.remove('row_last_modified')
    main_cols.remove('last_modified_date')
    territory_accumulation = territory_accumulation.withColumn('row_hash_multi',
                                                               F.sha2(F.concat_ws("||", *main_cols), 256))
    w_multi_rcd_sys = Window.partitionBy('row_hash_multi').orderBy(F.col('system_mod_stamp').desc(),
                                                                   F.col('row_end_date').desc())
    w_multi_rcd_start = Window.partitionBy('row_hash_multi').orderBy(F.col('row_start_date').asc())
    output_df_1 = territory_accumulation.withColumn('row_number', F.row_number().over(w_multi_rcd_sys))
    output_df_1 = output_df_1.filter(F.col('row_number') == 1)
    output_df_1 = output_df_1.drop('row_number', 'row_start_date')
    output_df_2 = territory_accumulation.withColumn('row_number', F.row_number().over(w_multi_rcd_start))
    output_df_2 = output_df_2.filter(F.col('row_number') == 1)
    output_df_2 = output_df_2.drop('row_number', 'row_end_date', 'row_source', 'row_hash',
                                   'row_is_current', 'row_operations', 'last_modified_date', 'row_last_modified',
                                   'system_mod_stamp', 'name', 'description', 'is_deleted',
                                   'country', 'territory_function', 'businessunit', 'id',
                                   'id_parent', 'salesforce', 'last_modified_by_id')
    output_df_3 = output_df_1.join(output_df_2, ['row_hash_multi'], "left")
    territory = safe_union(output_df_3, territory_hist)
    territory = territory.drop('row_hash_multi')
    # territory deduplication (temporary fix) - ends

    df = territory. \
        select(*INPUT_TERRITORY_SELECT)
    # adding this line since country is null for the top hierarchy
    df = df.withColumn('country', F.when(
        ((F.col('id_parent').isNull()) & (F.col('country').isNull())), F.lit('not available')).otherwise(
        F.col('country')))
    df = df.withColumn("row_start_date", F.to_date(F.col('row_start_date')))
    df = df.withColumn("row_end_date", F.to_date(F.col('row_end_date')))
    df = df.withColumnRenamed("name", "territory_id")
    df = df.withColumnRenamed("id", "veeva_territory_id")
    df = df.withColumnRenamed("businessunit", "business_unit")
    df = df.withColumnRenamed("country", "country_id")
    df = df.withColumnRenamed("description", "territory_description")
    df = df.withColumnRenamed("id_parent", "veeva_parent_territory_id")
    tl2 = territory.select(F.col("id").alias("veeva_parent_territory_id_clean"),
                           F.col("name").alias("parent_territory_id"),
                           F.col("row_start_date").alias("row_start_date_clean"),
                           F.col("row_end_date").alias("row_end_date_clean"))
    tl2_window = Window.partitionBy(*tl2_window)
    tl2 = tl2.withColumn("row_start_date_clean", F.min("row_start_date_clean").over(tl2_window)).withColumn(
        "row_end_date_clean", F.max("row_end_date_clean").over(tl2_window))
    # De-Duplication of Parent territory name
    df = df.join(tl2, (df.veeva_parent_territory_id == tl2.veeva_parent_territory_id_clean) &
                 (((F.col("row_start_date") >= F.to_date(F.col("row_start_date_clean"))) & (
                             F.col("row_end_date") <= F.to_date(F.col("row_end_date_clean")))) |
                  ((F.col("row_end_date") >= F.to_date(F.col("row_start_date_clean"))) & (
                              F.col("row_end_date") <= F.to_date(F.col("row_end_date_clean")))) |
                  ((F.col("row_start_date") >= F.to_date(F.col("row_start_date_clean"))) & (
                              F.col("row_start_date") <= F.to_date(F.col("row_end_date_clean"))))), 'left')

    window_territory = Window.partitionBy(*SELECT_COLS_PARTITION).orderBy(F.col("row_end_date_clean").desc())
    df = df.withColumn("rank", F.rank().over(window_territory))
    df = df.filter(F.col("rank") == 1)
    df = df.select(SELECT_COLS).dropDuplicates()

    # Scanidavia specific logic for demand DMND0295957
    scandinavia_countries = ['SC', 'NO']
    scandinavia_condition = [
        F.col("territory_id").startswith(s) &
        ((F.col('territory_id').like('%IM%') == True) |
         (F.col('territory_id').like('%SP%') == True) |
         (F.col('territory_id').like('%MED%') == True) |
         (F.col('territory_id').like('%ONC%') == True)) for s in scandinavia_countries
    ]
    country_not_set = df.filter(F.col('country_id').isNull()) \
        .withColumn('country_id', F.lit('not_set'))
    country_not_set = country_not_set.withColumn('affiliate', F.when(
        reduce(
            lambda x, y: x | y, scandinavia_condition, F.lit(False)),
        F.lit('Scandinavia')
    ).otherwise(F.lit(''))) \
        .withColumn('veeva_instance', F.lit('not_set'))

    df = standardize_country_code(df.filter(F.col('country_id').isNotNull()), iso_country_mapping) \
        .join(
        country.select('country_id', 'affiliate', 'veeva_instance'), 'country_id', 'left').unionByName(
        country_not_set
    )

    df = df.withColumn('territory_function', F.when(
        (F.col('territory_function').isNull()) & (F.col('country_id') == 'JPN') & (
                    F.col('territory_id').like('15%') == False), F.lit('Sales')
    ).when(
        (F.col('territory_function').isNull()) & (F.col('country_id') == 'JPN') & (F.col('territory_id').like('15%')),
        F.lit('MED')
    ).otherwise(F.col('territory_function')))
    """
    # data_quality_issue
    purpose:
    to map the column (Division(employee)->Business unit(Territory), Department(User)->Territory Function(Territory))
    DMND0283180 for reference
    """
    territory_russia = df.filter(F.col('country_id') == 'RUS')
    territory = df.filter(F.col('country_id') != 'RUS')
    employee = employee \
        .join(
        country.select('country_id', 'veeva_instance'),
        'country_id',
        'left') \
        .filter(
        (F.col('veeva_instance') == 'ROW') & (F.col('affiliate') != 'ANZ')
    )
    # TODO : This russia logic should be revisited , the values for business_unit and fuction should be based on the historical values
    # Sales_force needs to updated for all ROW instance , when sales_force is Null
    one_emp_many_territory = one_emp_many_territory \
        .join(
        country.select('country_id', 'veeva_instance'),
        'country_id',
        'left'
    ) \
        .filter(
        (F.col('row_is_current')) &
        (F.col('veeva_instance') == 'ROW') &
        (F.col('affiliate') != 'ANZ') &
        (F.col('veeva_territory_id') != 'NoTerritory')
    ) \
        .select('veeva_territory_id', 'employee_id', 'country_id').distinct()
    emp_territory = employee.join(
        one_emp_many_territory,
        one_emp_many_territory.employee_id == employee.employee_id,
        how='inner',
    ).select(
        employee.country_id.alias('country_id'),
        one_emp_many_territory.veeva_territory_id.alias("veeva_territory_id"),
        employee.division.alias("employee_business_unit"),
        employee.department.alias("employee_territory_function")
    ).dropDuplicates()
    update_window = Window.partitionBy('country_id', 'veeva_territory_id').orderBy(emp_territory.veeva_territory_id)
    # Russia requirement : technical territories to which multiple users  are assigned, like the one 0MI5J000000bxt2WAA . In this case the preferred division is customer excellence
    emp_territory = emp_territory. \
        withColumn('employee_business_unit', F.collect_set('employee_business_unit').over(update_window)). \
        withColumn('employee_territory_function', F.first('employee_territory_function').over(update_window)). \
        withColumn('employee_business_unit', F.when(
        ((F.col('country_id') == 'RUS') &
         (F.size('employee_business_unit') > 1) &
         (F.array_contains(F.col('employee_business_unit'), 'Customer Excellence'))), F.lit('Customer Excellence')
    ).when(
        F.size('employee_business_unit') == 0, F.lit(None)
    ).otherwise(F.col('employee_business_unit')[0])).dropDuplicates()
    territory_russia = territory_russia.join(
        emp_territory.filter(F.col('country_id') == 'RUS'),
        ['veeva_territory_id', 'country_id'],
        how='left',
    ).select(SELECT_FINAL_TERRITORY_COLS).dropDuplicates()
    territory_russia = territory_russia.withColumn('business_unit',
                                                   F.coalesce('business_unit', 'employee_business_unit')). \
        withColumn('territory_function', F.coalesce('territory_function', 'employee_territory_function'))
    # to derive the sales force for ROW instace
    territory = territory.join(
        emp_territory,
        ['veeva_territory_id', 'country_id'],
        how='left',
    )
    df = territory_russia.unionByName(territory)

    # salesforce Null , logic to update the salesforce based on territory
    # if '_' is there in territory , then first value would be team , if '_' not present salesforce would be country as in OneView pipeline
    # for ROW instance employees department will be sales force
    df = df.withColumn('salesforce', F.when(
        (F.col('salesforce').isNotNull()), F.col('salesforce')
    ).when(
        ((F.col('salesforce').isNull()) & (F.col('veeva_instance') == 'ROW') & (F.col('affiliate') != 'ANZ')),
        F.coalesce(F.col('employee_territory_function'), F.col('country_id'))
    ).when(
        ((F.col('salesforce').isNull()) & (F.col('territory_id').contains('_')) & (F.col('affiliate') == 'ANZ')),
        F.split(F.col('territory_id'), '_')[0]
    ).otherwise(
        F.col('country_id')
    )).drop('employee_territory_function', 'employee_business_unit')
    one_view_function = one_view_function.withColumn('oneview_function', F.lit(True))
    one_view_salesforce = one_view_salesforce.withColumn('oneview_salesforce', F.lit(True))
    df = df.join(
        F.broadcast(one_view_function),
        (one_view_function.country_id == df.country_id) & (
                    F.lower(one_view_function.territory_function) == F.lower(df.territory_function)),
        'left').select(df['*'], 'oneview_function')

    df = df.join(
        F.broadcast(one_view_salesforce),
        (one_view_salesforce.country_id == df.country_id) & (
                    F.lower(one_view_salesforce.salesforce) == F.lower(df.salesforce)),
        'left').select(df['*'], 'oneview_salesforce')
    df = one_view_filters(df, 'territory')
    df = gps_division(df, 'territory')
    df = add_cdc_cols(df, 'territory')
    df = df.drop("veeva_instance")

    return df


def product(**kwargs):
    """
    Products entity keeps information about pharma products in market (AbbVie and its competitors).
    Sources:
    - In Market Sales: AbbVie and its competitors sales.
    - SAP data: comes through direct sales data; maybe some day we will get direct access to product data.
    - Distributor Sales: should be the same as SAP data but it also provides extra information and fill gaps.
    - Veeva data: the way India stores product information, it needs for Distributor Sales project (Wave1).
    - Pricentric: products price data, it provides extra information and fill gaps.
    - Other sales plan data: the sources has data about products that are not sold yet.

    More information you can find here:
    https://btsconfluence.abbvie.com/display/OFUS/Ontology%3A+product
    """
    # ---------------------- Helpers ----------------------
    # Need to attach 3 chars country code
    country = kwargs['country']
    country_broadcast = F.broadcast(country.select('country_id', 'iso2').repartition(1))
    # Master data to enrich product records
    product_master_fdh = get_product_master_fdh(kwargs['product_master_fdh'])
    product_master_fdh_broadcast = F.broadcast(product_master_fdh).repartition(1)
    # Master data to enrich product records
    harmonized_product_hierarchy_fdh = get_harmonized_product_hierarchy_fdh(kwargs['harmonized_product_hierarchy_fdh'])
    # TODO the data is for enrichment; it needs to be moved out of helper functions
    harmonized_product_hierarchy_fdh_broadcast = F.broadcast(harmonized_product_hierarchy_fdh).repartition(1)
    # Master data to enrich product records
    reporting_product = get_reporting_product(kwargs['reporting_product'], country_broadcast)
    # TODO the data is for enrichment; it needs to be moved out of helper functions
    direct_sales_rp = (reporting_product.filter(F.col("source") == F.lit("Direct Sales"))
                       .drop('source')
                       .repartition(1)
                       )
    # ---------------------- Sales and other data ----------------------
    in_market_sales = kwargs['ims']
    direct_sales_products = kwargs['directsalesfact']
    ds_plan_local_item = kwargs['ds_plan_localitem']
    forecast_sales = kwargs['forecast_sales']
    pricing = kwargs['pricing']
    external_mapping = kwargs['external_mapping']
    iso_country_mapping = kwargs['iso_country_mapping']
    distributor_sales = kwargs['dist_sales']
    veeva_products = kwargs['product']
    allergan_product_master = kwargs['allergan_product_master']
    # HQPS sales benchmark data for abbvie/allergan
    hqps_allergan_pln_data = kwargs['hqps_allergan_pln_data']
    hqps_abbvie_upd_data = kwargs['hqps_abbvie_upd_data']
    hqps_abbvie_lbe_data = kwargs['hqps_abbvie_lbe_data']
    hqps_abbvie_pln_data = kwargs['hqps_abbvie_pln_data']

    # ---------------- Distributor sales ------------------- #
    distributor_sales_products = get_distributor_sales_products(distributor_sales, iso_country_mapping, direct_sales_rp)

    # ---------------- VEEVA INDIAN PRODUCTS ---------------- #
    indian_veeva_products = get_indian_veeva_products(veeva_products, reporting_product)
    # -------------------- IMS EXPORT ----------------------- #
    ims_product = get_ims_product(in_market_sales, reporting_product, iso_country_mapping)
    ################################################################################################
    # --------------- SAP DIRECT_SALES_FACT ---------------- #
    # Sales information (AbbVie -> Distribuots (or Customer; rarely)
    direct_sales_products = get_direct_sales_product(direct_sales_products,
                                                     country_broadcast,
                                                     product_master_fdh,
                                                     harmonized_product_hierarchy_fdh_broadcast,
                                                     direct_sales_rp)

    # --------------- ADW/CDW/JDA products --------------- #
    # Future sales
    # Read about ADW/CDW/JDA here:
    # TODO add links
    jda_products = get_jda_products(ds_plan_local_item,
                                    forecast_sales,
                                    allergan_product_master,
                                    country_broadcast,
                                    product_master_fdh_broadcast,
                                    harmonized_product_hierarchy_fdh_broadcast,
                                    direct_sales_rp)
    # Filter out direct sales
    jda_products = jda_products.join(direct_sales_products, on='product_id', how='left_anti')
    direct_sales_and_jda_products = union_different_df(direct_sales_products, jda_products)
    ################################################################################################

    # ---------------- PRICENTRIC ------------------- #
    # AbbVie and competitor products price information
    pricing = get_pricing(pricing, iso_country_mapping)

    # Pricentric products from SAP (mapped in the external sheet)
    external_mapping = (
        external_mapping.withColumn('pricentric_code', F.col('pricentric_code').cast('string'))
            .withColumn('pc_product_id', F.concat_ws('_', F.col('country_id'), F.col('pricentric_code')))
    )
    # remove the sap products that are available in SAP system from Pricentic
    pricing = pricing.alias('p').join(external_mapping.alias('e'),
                                      on=F.col('p.product_id') == F.col('e.pc_product_id'),
                                      how='left_anti')

    # -------------------------- HQPS data ------------------------------ #
    # HQPS provides sales plan and adjustment information
    hqps_abbvie_sales_target = union_different_df(hqps_abbvie_lbe_data, hqps_abbvie_pln_data, hqps_abbvie_upd_data)
    hqps_products = get_hpqs_products(reporting_product, country_broadcast, hqps_allergan_pln_data,
                                      hqps_abbvie_sales_target)
    # IMS source has same products , hence removing it to avoid duplicates;
    hqps_products = hqps_products.join(ims_product, 'product_id', 'left_anti')

    # -------------------------------------------------------- #
    # Union all datasets + general enrichment
    # -------------------------------------------------------- #
    # update the pricentric code for the sap products
    sap_pricentirc_pdts_df = sap_pricentirc_pdts(external_mapping)
    ims_product = ims_product.join(sap_pricentirc_pdts_df, on='product_id', how='left')
    # cleansing: remove, same products from SAP
    distributor_sales_products = distributor_sales_products.join(direct_sales_and_jda_products,
                                                                 on='product_id', how='left_anti')
    direct_sales_and_jda_products = direct_sales_and_jda_products.join(sap_pricentirc_pdts_df,
                                                                       on='product_id', how='left')
    all_datasets = [ims_product,
                    direct_sales_and_jda_products,
                    hqps_products,
                    distributor_sales_products,
                    indian_veeva_products,
                    pricing]

    products = union_different_df(*all_datasets)
    sap_product_code = F.coalesce('sap_product_code', 'sap_code')
    products = products.withColumn('sap_product_code', sap_product_code).drop('sap_code')
    products = gps_division(products, 'product')
    products = add_cdc_cols(products, 'product')
    products = get_gps_details(products, country)
    return products


def crm_stakeholders(**kwargs):
    calls = kwargs['calls']
    account = kwargs['account']
    multichannel_consent = kwargs['multichannel_consent']
    record_type = kwargs['record_type']
    customer_master = kwargs['customer_master']
    segmentation = kwargs['segmentation']
    vae = kwargs['vae']
    medical_insight = kwargs['medical_insight']
    survey = kwargs['survey']
    account_to_territory = kwargs['account_to_territory']
    account_to_account = kwargs['account_to_account']
    iso_country_mapping = kwargs['iso_country_mapping']
    address = kwargs['veeva_address']
    agn_sap_cust = kwargs['zoh_cust_ds']
    COLUMNS_TO_BE_RENAMED_ACCOUNT = [
        ("external_id", "level_0_source_id"),
        ("id", "veeva_id"),
        ("email", "email"),
        ("record_type_id", "record_type_id"),
        ("is_person_account", "is_person_account"),
        ("do_not_call", "do_not_call"),
        ("do_not_mail", "do_not_mail"),
        ("do_not_visit", "do_not_visit"),
        ("title", "title"),
        ("customer_type", "customer_type"),
        ('country', 'country_id'),
        ('row_is_current', 'row_is_current'),
        ('specialty_1', 'speciality_1'),
        ('specialty_2', 'speciality_2'),
        ('specialty_3', 'speciality_3'),
        ('abv_latam_kam_key_acc', 'is_key_account'),
        ('council_registration_number', 'registration_number'),
        ('professional_title', 'professional_title'),
        ('consent_email_refused', 'is_email_consent_refused'),
        ('status', 'status'),
        ('is_attachment', 'has_attachment'),
        ('count_attachment', 'number_of_attachments'),
        ('active_email_opt_out', 'active_email_opt_out'),
        ('parent_account_id', 'parent_account_id'),
        ('coder_pps', 'rpps_healthcare_id'),
        ('core_external_id_hrm1', 'adeli_healthcare_id'),
        ('cip_code_pharmacy', 'sales_point_id'),
        ('row_is_current', 'row_is_current'),
        ('row_last_modified', 'row_last_modified')
    ]

    # Missing Stakeholders columsn to selecte
    SELECT_COLUMNS_STAKEHOLDERS = [
        "country_id",
        "stakeholder_type",
        "stakeholder_id",
        "one_key_id",
        "veeva_account_id",
        "sap_customer_code",
        "stakeholder_name",
        "primary_speciality",  # language break
        "specialities",
        "last_update_date",
        "has_planned_visit",
        "customer_type",
        "is_person_account",
        "do_not_call",
        "do_not_mail",
        "do_not_visit",
        "title",
        "geohash",
        "latitude",
        "longitude",
        "email",
        'reltio_id',
        'last_modified_date',
        'status',
        'is_key_account',
        'registration_number',
        'professional_title',
        'is_email_consent_refused',
        'source',
        'has_attachment',
        'number_of_attachments',
        'active_email_opt_out',
        'parent_account_id',
        'rpps_healthcare_id',
        'adeli_healthcare_id',
        'sales_point_id',
        'row_is_current',
        'row_last_modified'

    ]
    # ******************* RECORD_TYPE ********************** #
    COLUMNS_TO_BE_RENAMED_RECORD_TYPE = [
        ("name", "customer_type"),
        ('id', 'record_type_id')
    ]

    COLUMNS_TO_BE_SELECTED_RECORD_TYPE = [col[1] for col in COLUMNS_TO_BE_RENAMED_RECORD_TYPE]
    # to get the reltio id using the external id mapped to account
    reltio_id_mapping = customer_master. \
        select(
        F.col('country_code').alias('reltio_country'),
        F.col('level_0_source_id').alias('external_id'),
        F.col('level_0_customer_id').alias('reltio_stakeholder_id'),
        'primary_affiliation'
    ).dropDuplicates()
    # to get the reltio id using the veeva id mapped to account
    customer_master = customer_master. \
        select(
        F.col('level_0_customer_id').alias('stakeholder_id'),
        F.col('level_0_veeva_foreign_key').alias('veeva_id'), 'geoloclati_longi_geoacc', F.col(
            'level_0_lanr_foreign_key').alias('lanr_id')). \
        dropDuplicates()
    customer_master = customer_master.groupBy('veeva_id', 'stakeholder_id', 'lanr_id'). \
        agg(F.first('geoloclati_longi_geoacc').alias('geoloclati_longi_geoacc'))
    account_to_account = account_to_account.withColumnRenamed('account_child', 'account_id')
    account = account.withColumn('stakeholder_id', F.split('reltio_mdm_uri', 'entities/')[1])
    # to fill email col with customised email col
    account = account.withColumn('email', F.coalesce('email', 'customized_email'))
    # 00107000004kpceAAA has no country in Veeva account table , temp fix to get country from reltio
    # Belgium local changes fix
    stakeholders_from_veeva = account.filter(F.col('row_is_current') == True). \
        join(reltio_id_mapping.drop('external_id').distinct(),
             account.stakeholder_id == reltio_id_mapping.reltio_stakeholder_id, 'left'). \
        withColumn('country',
                   F.when((F.col('country').isin(['BEL', 'LUX', 'LU', 'XXBE'])) & (F.col('primary_affiliation') == 'Y'),
                          F.col('reltio_country')).otherwise(F.coalesce('country', 'reltio_country'))). \
        drop('stakeholder_id', 'reltio_stakeholder_id', 'reltio_country', 'primary_affiliation')
    # to get the reltio_id from the Reltio table
    stakeholders_from_veeva = stakeholders_from_veeva.join(reltio_id_mapping, 'external_id', 'left')
    veeva_crm_list1 = [
        vae,
        segmentation,
        account_to_territory,

    ]

    veeva_crm_list2 = [
        multichannel_consent,
        survey,
        medical_insight,
        account_to_account,
        address

    ]
    stakeholders_in_crm = calls = calls.filter(F.col('row_is_current') == True). \
        select(F.col('account_id').alias('veeva_account_id')).distinct()

    for df in veeva_crm_list1:
        df = df.filter(F.col('is_deleted') == False). \
            select(F.col('account_id').alias('veeva_account_id')).distinct()
        stakeholders_in_crm = stakeholders_in_crm.unionByName(df).dropDuplicates()

    for df in veeva_crm_list2:
        df = df.filter(F.col('row_is_current') == True). \
            select(F.col('account_id').alias('veeva_account_id')).distinct()
        stakeholders_in_crm = stakeholders_in_crm.unionByName(df).dropDuplicates()
    # get the Veeva ID  those are not available in stakeholder
    # stakeholders_in_crm = stakeholders_in_crm.join(stakeholder, 'veeva_id', 'left_anti')
    stakeholders_in_crm = stakeholders_in_crm.filter(F.col('veeva_account_id').isNotNull())

    # Renaming columns in order to be merged
    for columns in COLUMNS_TO_BE_RENAMED_ACCOUNT:
        stakeholders_from_veeva = stakeholders_from_veeva.withColumnRenamed(columns[0], columns[1])

    # stakeholders_from_veeva = stakeholders_from_veeva.join(stakeholders_in_crm, 'veeva_id')

    stakeholders_from_veeva = standardize_country_code(
        stakeholders_from_veeva,
        iso_country_mapping
    )
    # Renaming columns in order to be merged
    for columns in COLUMNS_TO_BE_RENAMED_RECORD_TYPE:
        record_type = record_type.withColumnRenamed(columns[0], columns[1])
    # Customer Type for the stakekholder
    record_type = record_type.select(*COLUMNS_TO_BE_SELECTED_RECORD_TYPE)

    # joined with record_type ,to get the customer_type
    stakeholders_from_veeva = stakeholders_from_veeva \
        .join(
        F.broadcast(record_type),
        'record_type_id',
        'left'
    ).distinct()
    stakeholders_from_veeva = stakeholders_from_veeva.withColumn(
        'reltio_id',
        F.coalesce(F.split('reltio_mdm_uri', 'entities/')[1], F.col('reltio_stakeholder_id'), F.lit('no reltio id'))
    ).drop('reltio_stakeholder_id')
    agn_sap_cust = agn_sap_cust.select('CUSTOMER')
    stakeholders_from_veeva = stakeholders_from_veeva \
        .join(agn_sap_cust,
              stakeholders_from_veeva.level_0_source_id == agn_sap_cust.CUSTOMER,
              'left_outer'
              )
    # identify the sap customers
    stakeholders_from_veeva = stakeholders_from_veeva.withColumn('is_sap', (
                (F.col("level_0_source_id").startswith('005')) | (F.col('CUSTOMER').isNotNull()))) \
        .drop('CUSTOMER')
    # updating the sap records
    stakeholders_from_veeva = stakeholders_from_veeva. \
        withColumn('level_0_source_id', F.when(
        F.col('is_sap') == True, F.regexp_replace(stakeholders_from_veeva.level_0_source_id, r'^[0]*', '')
    ).otherwise(
        F.col('level_0_source_id')
    )
                   )

    # update the stakeholder_type
    stakeholders_from_veeva = stakeholders_from_veeva. \
        withColumn('stakeholder_type', F.when(
        F.col('is_person_account') == True, F.lit('Person')
    ).otherwise(
        F.lit('Organization')
    ))

    # update other externail ID  Onekey, veeva, sap
    stakeholders_from_veeva = stakeholders_from_veeva. \
        withColumn('one_key_id', F.when(F.col('is_sap') == True, F.lit('')).otherwise(F.col('level_0_source_id'))). \
        withColumn('veeva_account_id', F.col('veeva_id')). \
        withColumn('sap_customer_code',
                   F.when(F.col('is_sap') == True, F.col('level_0_source_id')).otherwise(F.lit('')))

    # general columns
    stakeholders_from_veeva = stakeholders_from_veeva. \
        withColumn('stakeholder_name', F.col('name')). \
        withColumn('last_update_date', F.date_format(F.to_date('last_modified_date'), 'YYYY-MM-dd')). \
        withColumnRenamed('is_primary', 'primary_affiliation')

    # update speciality columns - language break
    stakeholders_from_veeva = stakeholders_from_veeva. \
        withColumn('primary_speciality', F.col('speciality_1')). \
        withColumn('specialities', F.array(F.col('speciality_2'), F.col('speciality_3'))). \
        drop('speciality_2', 'speciality_3', 'speciality_1')

    # update has_planned_visit
    stakeholders_from_veeva = stakeholders_from_veeva. \
        withColumn('has_planned_visit', F.col('has_activity_cycle_plan')). \
        withColumn('stakeholder_id', F.col('reltio_id'))
    stakeholders_from_veeva = stakeholders_from_veeva.withColumn('source', F.lit('Veeva'))
    stakeholders_from_veeva = stakeholders_from_veeva. \
        join(customer_master.drop('veeva_id').dropDuplicates(),
             'stakeholder_id',
             'left'
             )
    stakeholders_from_veeva = add_geo_info(stakeholders_from_veeva)
    stakeholders_from_veeva_no_reltio_id = stakeholders_from_veeva.filter(
        F.col('reltio_id') == 'no reltio id'). \
        drop('stakeholder_id', 'lanr_id')
    stakeholders_from_veeva_no_reltio_id = stakeholders_from_veeva_no_reltio_id.join(customer_master, ['veeva_id'],
                                                                                     'left'). \
        withColumn('reltio_id', F.col('stakeholder_id')). \
        withColumn('source', F.when(F.col('stakeholder_id').isNotNull(), F.lit('Reltio')).otherwise(F.lit('Veeva'))). \
        withColumn('stakeholder_id', F.coalesce(F.col('reltio_id'), F.col('veeva_id')))

    # Add latitude and longitude columns, from geohash

    stakeholders_from_veeva_no_reltio_id = add_geo_info(stakeholders_from_veeva_no_reltio_id). \
        select(SELECT_COLUMNS_STAKEHOLDERS).distinct()
    stakeholder = stakeholders_from_veeva.filter(
        F.col('reltio_id') != 'no reltio id'). \
        select(SELECT_COLUMNS_STAKEHOLDERS).distinct(). \
        unionByName(stakeholders_from_veeva_no_reltio_id)
    stakeholder = stakeholder.select(SELECT_COLUMNS_STAKEHOLDERS).distinct()
    stakeholder = stakeholder.withColumn('is_key_account', F.when(F.col('is_key_account') == 1, True).otherwise(False))
    # find the unique stakeholder
    stakeholder = stakeholder.withColumn('stakeholder_count', F.count('stakeholder_id').over(
        Window.partitionBy('stakeholder_id').orderBy(stakeholder.stakeholder_id.asc())
    ))
    unique_stakeholders = stakeholder.filter(F.col('stakeholder_count') == 1).drop('stakeholder_count')
    # stakeholders having same reltio id with differnt veeva ID
    # NAWmue4	WDEM00541529	0010Y000013spd2QAA
    # NAWmue4	WDEM01989265	001G000001t4fjEIAQ
    stakeholder = stakeholder.filter(F.col('stakeholder_count') == 2)
    update_window = Window.partitionBy('stakeholder_id').orderBy(stakeholder.last_modified_date.desc())
    stakeholder = stakeholder.withColumn('rank', F.rank().over(update_window)
                                         )
    # based on the last modified date
    # NAWmue4 0010Y000013spd2QAA
    # 001G000001t4fjEIAQ 001G000001t4fjEIAQ
    stakeholder = stakeholder.withColumn('stakeholder_id', F.when(
        F.col('rank') == 1, F.col('stakeholder_id')).otherwise(F.col('veeva_account_id')))
    # considering only those stakeholders in Veeva CRM transaction data
    stakeholder = stakeholder.join(stakeholders_in_crm, 'veeva_account_id')
    # find the unique stakeholder
    stakeholder = stakeholder.withColumn('stakeholder_count', F.count('stakeholder_id').over(
        Window.partitionBy('stakeholder_id').orderBy(stakeholder.stakeholder_id.asc())
    ))
    unique_stakeholders_2 = stakeholder.filter(F.col('stakeholder_count') == 1).drop('stakeholder_count', 'rank')
    # Inactive	2	DEU	Organization	rskEEA9	WDEE00033522	0010Y00000mALUzQAO
    # Active	2	DEU	Organization	rskEEA9	WDEE00033521	0010Y00000mALUyQAO
    stakeholder = stakeholder.filter(F.col('stakeholder_count') == 2)
    # 0010Y00000mALUzQAO 0010Y00000mALUzQAO
    # 0010Y00000mALUyQAO 0010Y00000mALUyQAO
    unique_stakeholders_3 = stakeholder.withColumn('stakeholder_id', F.col('veeva_account_id')).drop(
        'stakeholder_count', 'rank')
    stakeholders = unique_stakeholders.unionByName(unique_stakeholders_2).unionByName(unique_stakeholders_3)
    return stakeholders.drop('last_modified_date')


def add_geo_info(df):
    df = df. \
        withColumn("geoloclati_longi_geoacc", F.split("geoloclati_longi_geoacc", '\^'))

    df = df. \
        withColumn("geohash", F.concat_ws(
        ',', F.element_at("geoloclati_longi_geoacc", 1), F.element_at("geoloclati_longi_geoacc", 2))
                   )
    # Add latitude and longitude columns, from geohash
    df = df. \
        withColumn("latitude", F.regexp_extract("geohash", '^[^,]*', 0))
    df = df. \
        withColumn("longitude", F.regexp_extract("geohash", '[^,]*$', 0)). \
        drop('geoloclati_longi_geoacc')
    return df


def stakeholder(**kwargs):
    all_stakeholders = kwargs['all_stakeholders']
    iso_country_mapping = kwargs['iso_country_mapping']
    registrants = kwargs['registrants']
    pricing = kwargs['pricing']
    country = kwargs['country']
    dist_sales = kwargs['dist_sales']
    account = kwargs['account']

    # Columns to be selected country
    COLUMNS_TO_BE_SELECTED_COUNTRY = [
        'country_id',
        'country_name',
        'affiliate'
    ]

    # Columns to be selected from pricing
    COLUMNS_TO_BE_RENAMED_PRICING_RECORDS = [
        ("country", "country_id"),
        ("company", "company"),
        ("row_is_current", "row_is_current"),
        ("row_last_modified", "row_last_modified")
    ]

    # ******************* INDIAN ACCOUNTS ********************** #
    account = (account.filter(F.col('country') == 'IND')
               .withColumn('stakeholder_id', F.col('id'))
               .withColumn('external_veeva_id', F.col('external_id'))
               .select('stakeholder_id', 'external_veeva_id', 'wholesaler_division')
               )

    # ******************* Distributor Sales ********************** #
    COLUMNS_TO_BE_RENAMED_DIS_SALES = [
        ("sold_to_customer_id", "stakeholder_id"),
        ('sold_to_customer_name', 'stakeholder_name'),
        ('country', 'country_id'),
        ("row_is_current", "row_is_current"),
        ("row_last_modified", "row_last_modified")
    ]

    COLUMNS_TO_BE_SELECTED_PRICING_RECORDS = [col[1] for col in COLUMNS_TO_BE_RENAMED_PRICING_RECORDS]
    COLUMNS_TO_BE_SELECTED_DIST_SALES = [col[1] for col in COLUMNS_TO_BE_RENAMED_DIS_SALES]
    # ************** Select columns from country *************************#
    country = country.select(COLUMNS_TO_BE_SELECTED_COUNTRY).distinct()

    # ----- PREPARE STAKEHOLDERS FROM WEBINAR ----- #
    on_24_stakeholders = prepare_on24_stakeholders(registrants, iso_country_mapping, all_stakeholders, country)

    # Merge ON24 stakeholders
    stakeholder = safe_union(all_stakeholders, on_24_stakeholders)
    # country_id that are null will be filteres in the standardizing of country
    # ----- PREPARE STAKEHOLDERS FROM WEBINAR ENDS -----
    # standardize the HCP- Person and HCO- Organization

    stakeholder = stakeholder \
        .withColumn('stakeholder_type', F.when(
        F.col('stakeholder_type') == 'Person', F.lit('person')
    ).otherwise(F.lit('organization'))
                    )

    # ---------------- Organizations from PRICENTRIC ------------------- #
    # Renaming columns in order to be merged
    for columns in COLUMNS_TO_BE_RENAMED_PRICING_RECORDS:
        pricing = pricing.withColumnRenamed(columns[0], columns[1])

    # Select only a subset of columns
    pricing = pricing.select(
        *COLUMNS_TO_BE_SELECTED_PRICING_RECORDS)
    pricing = standardize_country_code(pricing, iso_country_mapping)
    # Standardize the country column
    organization_from_pricing = pricing. \
        withColumn(
        'stakeholder_id', F.concat_ws('_', *['country_id', 'company'])
    ). \
        withColumn('stakeholder_type', F.lit('organization')). \
        withColumn('source', F.lit('Pricentric')). \
        withColumn('stakeholder_name', F.col('company')). \
        drop('company').distinct()

    stakeholder = safe_union(stakeholder, organization_from_pricing)
    stakeholder = standardize_country_code(stakeholder, iso_country_mapping)
    stakeholder = stakeholder.join(
        all_stakeholders.select(
            F.col('stakeholder_id').alias('parent_stakeholder_id'),
            F.col('veeva_account_id').alias('parent_account_id')),
        'parent_account_id',
        'left')
    # Distributor sales integration
    for columns in COLUMNS_TO_BE_RENAMED_DIS_SALES:
        dist_sales = dist_sales.withColumnRenamed(columns[0], columns[1])
    dist_sales = dist_sales.select(
        *COLUMNS_TO_BE_SELECTED_DIST_SALES)
    dist_sales = standardize_country_code(dist_sales, iso_country_mapping)
    dist_sales = dist_sales.withColumn('stakeholder_type', F.lit('organization')). \
        withColumn('is_person_account', F.lit(False)). \
        withColumn('sap_customer_code', F.col('stakeholder_id')). \
        withColumn('source', F.lit('distributor_sales')). \
        dropDuplicates()
    stakeholder = safe_union(stakeholder, dist_sales)
    stakeholder = get_gps_details(stakeholder, country)
    stakeholder = add_cdc_cols(stakeholder, 'stakeholder')
    stakeholder = gps_division(stakeholder, 'stakeholder')
    stakeholder = one_view_filters(stakeholder, 'stakeholder')
    stakeholder = stakeholder.join(account, 'stakeholder_id', 'left')
    return stakeholder.repartition(10)


def prepare_on24_stakeholders(registrants, iso_country_mapping, stakeholder, country):
    # ******************* REGISTRANTS ********************** #
    COLUMNS_TO_BE_RENAMED_REGISTRANTS = [
        ("email", "email"),
        ('job_title', 'title'),
        ('first_name', 'first_name'),
        ('last_name', 'last_name'),
        ('specialty', 'primary_speciality'),
        ('country', 'country_id'),
        ('account_id', 'veeva_account_id'),
        ('event_id', 'event_id'),
        ('event_user_id', 'event_user_id'),
        ('row_is_current', 'row_is_current'),
        ('row_last_modified', 'row_last_modified')
    ]
    COLUMNS_TO_BE_SELECTED_REGISTRANTS = [col[1] for col in COLUMNS_TO_BE_RENAMED_REGISTRANTS]
    #  Stakeholders who registered for the webminars
    registrants = registrants.withColumn('country', F.coalesce('country', 'registrant_country'))
    for columns in COLUMNS_TO_BE_RENAMED_REGISTRANTS:
        registrants = registrants.withColumnRenamed(columns[0], columns[1])

    registrants = standardize_country_code(registrants, iso_country_mapping).drop('registrant_country')
    registrants = registrants.filter(F.col('country_id').isNotNull())
    registrants = registrants.select(
        COLUMNS_TO_BE_SELECTED_REGISTRANTS
    ).distinct()
    # find the stakeholders those are available from registrants
    registrants_with_id = registrants.filter(
        F.col('veeva_account_id').isNotNull()
    )
    # account_id is not updated for few registrants
    registrants_without_id = registrants.filter(
        F.col('veeva_account_id').isNull() &
        F.col('email').isNotNull()
    )
    # registrants with veeva_account_id
    on24_stakeholders = registrants_with_id.join(
        stakeholder.select('veeva_account_id'),
        'veeva_account_id'
    )
    # exclude registrants who are available in stakeholder
    registrants_with_id = registrants_with_id.join(
        on24_stakeholders,
        ['event_id', 'event_user_id'],
        'left_anti'
    )
    # union all registrants
    registrants = registrants_without_id.unionByName(
        registrants_with_id
    ).withColumnRenamed('veeva_account_id', 'on24_id')

    registrants = registrants.withColumn(
        'stakeholder_id', F.coalesce('on24_id', F.concat_ws('_', *['country_id', 'email']))
    ).withColumn('stakeholder_name', F.concat(
        F.coalesce('first_name', F.lit('')),
        F.coalesce('last_name', F.lit(''))
    ))
    # get the unique registrants details

    w = Window.partitionBy('stakeholder_id').orderBy(registrants.event_id.desc())

    registrants = registrants.withColumn("rank", F.rank().over(w))
    registrants = registrants.where(registrants.rank == '1').drop('rank')  # latest records

    registrants = registrants. \
        groupBy('stakeholder_id', 'country_id', 'row_is_current'). \
        agg(
        F.collect_set('title')[0].alias('title'),
        F.collect_list('stakeholder_name')[0].alias('stakeholder_name'),
        F.collect_set('primary_speciality')[0].alias('primary_speciality'),
        F.first('email').alias('email'),
        F.max('row_last_modified').alias('row_last_modified')
    )
    registrants = registrants. \
        withColumn('stakeholder_type', F.lit('Person')). \
        withColumn('has_planned_visit', F.lit(False)). \
        withColumn('is_person_account', F.lit(True)). \
        withColumn('do_not_call', F.lit(False)). \
        withColumn('do_not_mail', F.lit(False)). \
        withColumn('do_not_visit', F.lit(False)). \
        withColumn('source', F.lit('ON24'))
    registrants = registrants.dropDuplicates()
    return registrants


def sap_account(**kwargs):
    customer = kwargs['customer_input']

    # Create SAP ID and reltio ID mapping
    sap_mapping = customer.where(
        customer.level_0_sap_foreign_key.isNotNull()
        & customer.reporting_account_customer_id.isNotNull()
    ).select(
        F.regexp_replace(customer.level_0_sap_foreign_key, r'^[0]*', '').alias('sap_id'),
        customer.reporting_account_customer_id.alias('account_id')
    ).groupBy('account_id').agg(F.collect_set(F.col('sap_id')).alias('sap_ids'))

    return sap_mapping


def person(**kwargs):
    account = kwargs['account']
    stakeholder = kwargs['stakeholder']
    record_type = kwargs['record_type']
    sap_mapping = kwargs['sap_mapping']
    country = kwargs['country']
    account_consent = kwargs['account_consent']
    COLUMNS_TO_BE_RENAMED_ACCOUNT = [
        ("professional_title", "professional_title"),
        ('external_expert_flag', 'is_external_expert'),
        ('record_type_id', 'record_type_id'),
        ('id', 'veeva_account_id'),
        ("external_id", "external_id"),
        ("phone", "phone"),
        ("fax", "fax"),
        ("first_name", "first_name"),
        ("gender", "gender"),
        ("is_investigator", "is_investigator"),
        ("is_person_opted_out_of_email", "is_person_opted_out_of_email"),
        ("is_person_opted_out_of_fax", "is_person_opted_out_of_fax"),
        ("last_name", "last_name"),
        ("owner_id", "employee_id"),
        ("status", "status"),
        ("salutation", "salutation"),
        ("title", "role"),
        ("civility", "civility"),
        ("type_of_structure", "type_of_structure"),
        ("category", "category"),
        ("council_registration_number", "registration_number"),
        ("abv_latam_kam_key_acc", "is_key_account"),
        ("specialty_1", "specialty_1"),
        ("specialty_2", "specialty_2"),
        ("specialty_3", "specialty_3"),
        ("has_activity_cycle_plan", "has_cycle_plan"),
        ("status_one_key", "validation_status"),
        ("workplace_type_label", "workplace_type"),
        ('consent_email_refused', 'is_email_consent_refused'),
        ('parent_account_id', 'parent_account_id'),
        ('name', 'name'),
        ('active_email_opt_in', 'is_email_opt_in'),
        ('active_email_opt_out', 'is_email_opt_out'),
        ('row_is_current', 'row_is_current'),
        ('row_last_modified', 'row_last_modified')
    ]

    # ******************* RECORD_TYPE ********************** #
    COLUMNS_TO_BE_RENAMED_RECORD_TYPE = [
        ("name", "record_type_name"),
        ('id', 'record_type_id')
    ]

    COLUMNS_TO_SELECTED = [
        "record_type_id",
        "veeva_account_id",
        "person_id",
        "source",
        "one_key_id",
        "parent_account_id",
        "country_id",
        "name",
        "specialty_1",
        "specialty_2",
        "specialty_3",
        "has_cycle_plan",
        "validation_status",
        "workplace_type",
        "sap_ids",
        "professional_title",
        "is_external_expert",
        "external_id",
        "phone",
        "fax",
        "first_name",
        "gender",
        "is_investigator",
        "is_person_opted_out_of_email",
        "is_person_opted_out_of_fax",
        "last_name",
        "employee_id",
        "status",
        "salutation",
        "role",
        "civility",
        "type_of_structure",
        "category",
        "registration_number",
        "is_key_account",
        "is_email_consent_refused",
        "record_type_name",
        "stakeholder_id",
        "parent_stakeholder_id",
        "rpps_healthcare_id",
        "adeli_healthcare_id",
        "is_email_opt_in",
        "is_email_opt_out",
        "row_is_current",
        "row_last_modified",
        "has_provided_consent"
    ]

    account_consent = account_consent.select("account_id", "consent_period", "has_provided_consent", 'consent_type',
                                             'row_last_modified').filter(F.col('consent_type').isNotNull())
    account_consent = account_consent.withColumn('row_num', F.row_number().over(
        Window.partitionBy("account_id").orderBy(F.col('row_last_modified').desc(), F.col('consent_period').desc())))
    account_consent = account_consent.filter(F.col("row_num") == 1).drop("row_num",
                                                                         'row_last_modified').withColumnRenamed(
        'account_id', 'veeva_account_id')
    account_consent = account_consent.withColumn("has_provided_consent",
                                                 F.when(F.col("has_provided_consent") == 'Yes', True).otherwise(False))

    COLUMNS_TO_BE_SELECTED_ACCOUNT = [col[1] for col in COLUMNS_TO_BE_RENAMED_ACCOUNT]
    COLUMNS_TO_BE_SELECTED_RECORD_TYPE = [col[1] for col in COLUMNS_TO_BE_RENAMED_RECORD_TYPE]

    # rename the columns
    account = account.drop('workplace_type')
    for columns in COLUMNS_TO_BE_RENAMED_ACCOUNT:
        account = account.withColumnRenamed(columns[0], columns[1])

    account = account.select(
        COLUMNS_TO_BE_SELECTED_ACCOUNT
    ).distinct()

    # Add SAP ID
    account = account.join(F.broadcast(sap_mapping.withColumnRenamed('account_id', 'veeva_account_id')),
                           ['veeva_account_id'], 'left')
    # Renaming columns in order to be merged
    for columns in COLUMNS_TO_BE_RENAMED_RECORD_TYPE:
        record_type = record_type.withColumnRenamed(columns[0], columns[1])
    # Customer Type for the stakekholder
    record_type = record_type.select(*COLUMNS_TO_BE_SELECTED_RECORD_TYPE)

    # joined with record_type ,to get the customer_type
    account = account \
        .join(
        F.broadcast(record_type),
        'record_type_id',
        'left'
    ).distinct()
    account = account.join(
        stakeholder.select(
            'stakeholder_id', 'veeva_account_id', 'country_id',
            'stakeholder_type', 'source', 'one_key_id',
            'parent_stakeholder_id', 'rpps_healthcare_id', 'adeli_healthcare_id'
        ), 'veeva_account_id')

    account = account.withColumn(
        'is_external_expert', F.when(F.col('is_external_expert') == 1, F.lit(True)).otherwise(F.lit(False))
    )
    account = account.join(account_consent, 'veeva_account_id', 'left')
    person = account.where(account.stakeholder_type == 'person').drop('stakeholder_type')
    person = person.withColumn('is_email_opt_out', F.col('is_email_opt_out').cast(T.BooleanType()))
    person = person.withColumn('person_id', F.col('stakeholder_id')).select(COLUMNS_TO_SELECTED)
    person = get_gps_details(person, country)
    person = gps_division(person, 'person')
    return person


def organization(**kwargs):
    sap_mapping = kwargs['sap_mapping']
    pricing = kwargs['pricing']
    iso_country_mapping = kwargs['iso_country_mapping']
    account = kwargs['account']
    stakeholder = kwargs['stakeholder']
    record_type = kwargs['record_type']
    country = kwargs['country']
    # Columns to be selected from pricing
    COLUMNS_TO_BE_RENAMED_PRICING_RECORDS = [
        ("country", "country_id"),
        ("company", "company"),
        ("row_is_current", "row_is_current"),
        ("row_last_modified", "row_last_modified")
    ]

    COLUMNS_TO_BE_SELECTED_PRICING_RECORDS = [col[1] for col in COLUMNS_TO_BE_RENAMED_PRICING_RECORDS]

    COLUMNS_TO_BE_RENAMED_ACCOUNT = [
        ("professional_title", "professional_title"),
        ('external_expert_flag', 'is_external_expert'),
        ('record_type_id', 'record_type_id'),
        ('id', 'veeva_account_id'),
        ("external_id", "external_id"),
        ("phone", "phone"),
        ("fax", "fax"),
        ("first_name", "first_name"),
        ("gender", "gender"),
        ("is_investigator", "is_investigator"),
        ("is_person_opted_out_of_email", "is_person_opted_out_of_email"),
        ("is_person_opted_out_of_fax", "is_person_opted_out_of_fax"),
        ("last_name", "last_name"),
        ("owner_id", "employee_id"),
        ("status", "status"),
        ("salutation", "salutation"),
        ("title", "role"),
        ("civility", "civility"),
        ("record_type_id", "record_type_id"),
        ("type_of_structure", "type_of_structure"),
        ("category", "category"),
        ("council_registration_number", "registration_number"),
        ("abv_latam_kam_key_acc", "is_key_account"),
        ("specialty_1", "specialty_1"),
        ("specialty_2", "specialty_2"),
        ("specialty_3", "specialty_3"),
        ("has_activity_cycle_plan", "has_cycle_plan"),
        ("status_one_key", "validation_status"),
        ("workplace_type_label", "workplace_type"),
        ('consent_email_refused', 'is_email_consent_refused'),
        ("name", "name"),
        ("parent_account_id", "parent_account_id"),
        ("row_is_current", "row_is_current"),
        ("row_last_modified", "row_last_modified")
    ]

    COLUMNS_TO_SELECTED_ORGANIZATION = [
        "organization_id",
        "source",
        "one_key_id",
        "veeva_account_id",
        "parent_account_id",
        "country_id",
        "name",
        "specialty_1",
        "specialty_2",
        "specialty_3",
        "has_cycle_plan",
        "validation_status",
        "workplace_type",
        "sap_ids",
        "status",
        "phone",
        "fax",
        "record_type_id",
        "record_type_name",
        "civility",
        "is_key_account",
        "type_of_structure",
        "category",
        "registration_number",
        "stakeholder_id",
        "parent_stakeholder_id",
        "sales_point_id",
        "row_is_current",
        "row_last_modified"
    ]

    # ******************* RECORD_TYPE ********************** #
    COLUMNS_TO_BE_RENAMED_RECORD_TYPE = [
        ("name", "record_type_name"),
        ('id', 'record_type_id')
    ]

    COLUMNS_TO_BE_SELECTED_ACCOUNT = [col[1] for col in COLUMNS_TO_BE_RENAMED_ACCOUNT]
    COLUMNS_TO_BE_SELECTED_RECORD_TYPE = [col[1] for col in COLUMNS_TO_BE_RENAMED_RECORD_TYPE]

    account = account.drop('workplace_type')
    # rename the columns
    for columns in COLUMNS_TO_BE_RENAMED_ACCOUNT:
        account = account.withColumnRenamed(columns[0], columns[1])

    account = account.select(
        COLUMNS_TO_BE_SELECTED_ACCOUNT
    ).distinct()

    # Add SAP ID
    account = account.join(F.broadcast(sap_mapping.withColumnRenamed('account_id', 'veeva_account_id')),
                           ['veeva_account_id'], 'left')
    # standardize the HCP- Person and HCO- Organization

    account = account.join(
        stakeholder.filter(F.col('stakeholder_type') == 'organization').select(
            'stakeholder_id', 'veeva_account_id', 'country_id', 'source', 'one_key_id',
            'parent_stakeholder_id', 'sales_point_id'
        ),
        'veeva_account_id')

    for columns in COLUMNS_TO_BE_RENAMED_RECORD_TYPE:
        record_type = record_type.withColumnRenamed(columns[0], columns[1])
    record_type = record_type.select(*COLUMNS_TO_BE_SELECTED_RECORD_TYPE)

    # joined with record_type ,to get the customer_type
    account = account \
        .join(
        F.broadcast(record_type),
        'record_type_id',
        'left'
    ).distinct()
    organization = account.withColumn(
        'is_external_expert', F.when(F.col('is_external_expert') == 1, F.lit(True)).otherwise(F.lit(False))
    )
    # ---------------- Organizations from PRICENTRIC ------------------- #
    # Renaming columns in order to be merged
    for columns in COLUMNS_TO_BE_RENAMED_PRICING_RECORDS:
        pricing = pricing.withColumnRenamed(columns[0], columns[1])

    # Select only a subset of columns
    pricing = pricing.select(
        *COLUMNS_TO_BE_SELECTED_PRICING_RECORDS)

    # Standardize the country column
    organization_from_pricing = standardize_country_code(pricing, iso_country_mapping)
    organization_from_pricing = organization_from_pricing. \
        withColumn(
        'stakeholder_id', F.concat_ws('_', *['country_id', 'company'])
    ). \
        withColumn('source', F.lit('Pricentric')). \
        withColumn('name', F.col('company')). \
        drop('company').distinct()

    organization = safe_union(organization, organization_from_pricing)
    organization = organization.withColumn('organization_id', F.col('stakeholder_id'))
    organization = organization.select(COLUMNS_TO_SELECTED_ORGANIZATION).distinct()
    organization = add_cdc_cols(organization, 'organization')
    organization = gps_division(organization, 'organization')
    return get_gps_details(organization, country)


def employee(**kwargs):
    employee = kwargs['employee']
    iso_country_mapping = kwargs['iso_country_mapping']
    country = kwargs['country']

    # Columns to drop in Employee
    COLUMNS_TO_DROP_IN_EMPLOYEE = [
        'available',
        'city',
        'company_name',
        'country_flag_list',
        'created_by_id',
        'customized_primary_territory',
        'digest_frequency',
        'email_encoding_key',
        'expiration_date_fsb',
        'extension',
        'fax',
        'full_photo_url',
        'last_modified_by_id',
        'last_modified_date',
        'last_referenced_date',
        'region',
        'system_mod_stamp',
        'user_type',
        'country_flag4',
        'country_flag5',
        'secondary_country_flag',
        'tertiary_country_flag'
    ]

    COLUMNS_TO_BE_RENAMED_EMPLOYEE = [
        ("id", "employee_id"),
        ("country", "country_id"),
        ("company_user_id2", "organization_id"),
        ('last_login_date', 'last_login_date')
    ]
    employee = employee.withColumn('primary_territory', F.coalesce('primary_territory', 'customized_primary_territory'))

    # Renaming columns
    for columns in COLUMNS_TO_BE_RENAMED_EMPLOYEE:
        employee = employee.withColumnRenamed(columns[0], columns[1])

    # drop columns that are not required
    employee = employee.select(
        *[col for col in employee.columns if col not in COLUMNS_TO_DROP_IN_EMPLOYEE]
    )
    # if in_filed is 100 , then user is in In Field
    employee = employee.withColumn('is_ift_member',
                                   F.when(F.col('core_in_field_dedication') == 100, True).otherwise(False))
    employee = employee.withColumn('country_id', F.upper(F.col('country_id')))
    # standardize 3 digit country code
    employee = standardize_country_code(
        employee,
        iso_country_mapping
    )
    employee = get_gps_details(employee, country)
    employee = gps_division(employee, 'employee')
    return employee


def employee_activity(**kwargs):
    activities = kwargs['activities']
    record_type = kwargs['record_type']
    country = kwargs['country']
    mapping_hours_time = kwargs['mapping_hours_time']
    mapping_hours_activity_type = kwargs['mapping_hours_activity_type']
    contact = kwargs['contact']
    calls = kwargs['calls']
    employee = kwargs['employee']
    mapping_activity_type = kwargs['mapping_activity_type']
    SELECT_COLUMNS = [
        'id',
        'country',
        'owner_id',
        'territory_id',
        'name',
        'reason',
        'is_all_user',
        'is_deleted',
        'is_including_weekends',
        'status',
        'time',
        'start_time',
        'start_date',
        'end_date',
        'hours',
        'hours_off',
        'number_of_days',
        'create_date',
        'date',
        'record_type_id',
        'comments',
        'city',
        'region',
        'type_of_transport',
        'row_is_current',
        'row_last_modified'
    ]

    ACTIVITY_TPE_SELECT_COLS = [
        'activity_type',
        'activity_cluster',
        'activity_global_cluster',
        'activity_global_cluster_number',
        'is_full_time_employee'
    ]
    # filters applied are for cleanup
    output_df = activities \
        .filter(
        (F.col('row_is_current')) &
        (F.year(F.col('date')) >= F.year(F.col('date')) - 3) &
        (F.col('date') <= F.current_date()) &
        (F.col('reason').isNotNull())
    ) \
        .select(SELECT_COLUMNS) \
        .withColumnRenamed('owner_id', 'employee_id') \
        .withColumnRenamed('country', 'country_id') \
        .withColumn('territory_id', F.array_remove(F.split('territory_id', ';'), '')) \
        .withColumn(
        "reason_type",
        F.when(
            F.col('reason').contains('-'),
            F.trim(F.split('reason', '-')[0])
        )) \
        .withColumn(
        "reason_classification",
        F.when(
            F.col('reason').contains('-'),
            F.trim(F.split('reason', '-')[1])
        ).otherwise(F.col('reason'))) \
        .withColumn('is_vacant_territory', F.when(
        F.col('reason') == 'OA - Vacant Territory', F.lit(True)
    ).otherwise(F.lit(False))) \
        .withColumn('activity_hours', F.col('hours')) \
        .withColumn('activity_type', F.col('reason')) \
        .withColumn('date', F.to_date('date')) \
        .withColumn('is_time_off', F.lit(True))
    output_df = get_record_type_details(record_type, output_df). \
        withColumnRenamed('id', 'employee_activity_id')
    ###############
    # CLEANING #
    ###############
    # 1) activity_hours: when there are no hours available (hours = 0 or null), then find hours based on column 'time' and based on hours-mapping-tables, otherwise 0  # noqa
    output_df = output_df \
        .withColumn('activity_hours',
                    F.when(F.col('activity_hours') == F.lit(0), F.lit(None)).otherwise(F.col('activity_hours'))) \
        .join(mapping_hours_time, 'time', 'left') \
        .withColumn('activity_hours', F.when(F.col('activity_hours').isNull(),
                                             F.col('hours_corrected')).otherwise(
        F.col('activity_hours'))) \
        .drop('id', 'hours_corrected', ) \
        .join(mapping_hours_activity_type, 'activity_type', 'left') \
        .withColumn('activity_hours', F.when(F.col('activity_hours').isNull(),
                                             F.col('hours_corrected')).otherwise(
        F.col('activity_hours'))) \
        .drop('id', 'hours_corrected') \
        .withColumn('activity_hours', F.when(F.col('activity_hours').isNull(),
                                             F.lit(0)).otherwise(
        F.col('activity_hours')))

    # TO_DO: This filter should be revistied
    # contact days , those event that are actual calls
    # 'Event_vod'
    contact_df = contact. \
        filter((F.col('employee_id').isNotNull()) & (F.col('status') == 'Submitted') &
               (F.col('employee_event') == 'Owner') &
               ((F.col('contact_date') >= '2018-01-01') & (F.col('contact_date') <= F.current_date())) &
               (F.col("sub_type").isin([
                   'Call Report Pharmacy',
                   'Call Report_vod',
                   'Engage_Meeting',
                   'Call Report (AUS/NZL)',
                   'Call Report LATAM',
                   'Interaction Report',
               ])
               )). \
        select(
        'country_id',
        F.col('contact_id').alias('employee_activity_id'),
        'employee_id',
        F.array('territory_id').alias('territory_id'),
        F.col('contact_date').alias('date'),
        F.lit(0).cast('decimal(28,0)').alias('hours'),  # not applicable
        F.lit(0).cast('decimal(28,0)').alias('activity_hours'),  # not applicable
        (F.col('contact_duration') / 60).alias('field_hours'),  # In call
        F.lit('Contact Days').alias('reason'),
        F.lit('Contact Days').alias('activity_type'),
        F.lit(False).alias('is_vacant_territory'),
        F.col('description'),
        F.col('record_type'),
        F.lit(False).alias('is_time_off'),
        F.col('row_is_current'),
        F.col('row_last_modified')
    )
    contact_df = contact_df.join(
        (calls.filter(F.col('row_is_current') == True).
         withColumnRenamed('id', 'employee_activity_id').
         select(['employee_activity_id', 'datetime', 'end_time'])
         ),
        'employee_activity_id',
        'left')

    # 2) am/pm: set and check start_time for contacts and set 'AM' or 'PM'
    def get_start_time(df):
        df = df.join(employee.select(['employee_id', 'timezones_id_key']), 'employee_id', 'left')
        # find the offset hrs to add from GMT time to get the local time
        # getting the local time and the getting the diff from GMT to get the correct offset hours( it will be helpfull both during DST and non DST periods)
        df = df.withColumn('local_time', F.from_utc_timestamp(F.col('datetime'), F.col('timezones_id_key')))
        df = df.withColumn('offset', ((F.col('local_time').cast("long") - F.col('datetime').cast("long")) / 60 / 60))
        df = df \
            .withColumn('start_time',
                        (F.unix_timestamp(F.col('datetime'), 'yyyy-MM-dd HH:mm:ss') + F.col('offset') * 60 * 60).cast(
                            T.TimestampType())) \
            .withColumn('end_time',
                        (F.unix_timestamp(F.col('end_time'), 'yyyy-MM-dd HH:mm:ss') + F.col('offset') * 60 * 60).cast(
                            T.TimestampType())) \
            .withColumn('am_pm', F.when(F.hour(F.col('start_time')) < 12,
                                        F.array(F.lit('AM'))).otherwise(
            F.array(F.lit('PM')))) \
            .drop('datetime', 'timezones_id_key', 'offset', 'local_time')
        return df

    contact_df = get_start_time(contact_df)

    # 3) event_hours/field_hours: when there is no duration available (duration = 0 or null), then calculate duration based on columns 'end_time' minus 'start_time', otherwise 0.5 (30 minutes)  # noqa
    def clean_contact_hours(df, column_name):
        return df \
            .withColumn(column_name, F.when(F.col(column_name) == F.lit(0), F.lit(None)).otherwise(F.col(column_name))) \
            .withColumn(column_name, F.when(F.col(column_name).isNull(),
                                            ((F.col('end_time').cast(T.LongType()) - F.col('start_time').cast(
                                                T.LongType())) / 60 / 60)).otherwise(  # noqa
            F.col(column_name))) \
            .withColumn(column_name, F.when(F.col(column_name).isNull(),
                                            F.lit(0.5)).otherwise(
            F.col(column_name))) \
            .drop('end_time', 'am_pm') \
            .withColumnRenamed('start_time', 'field_time')

    contact_df = clean_contact_hours(contact_df, 'field_hours')

    # 4) activity/activity_type: correct activity_type-names based on activity_type-mapping-table
    activity_df = output_df \
        .join(mapping_activity_type.select(['activity_type', 'activity_type_corrected']), 'activity_type', 'left') \
        .drop('activity_type') \
        .withColumnRenamed('activity_type_corrected', 'activity_type') \
        .drop('field_id')
    df = safe_union(activity_df, contact_df)

    # add activity_type mapping
    df = df \
        .join(mapping_activity_type
              .select(ACTIVITY_TPE_SELECT_COLS),
              'activity_type', 'left')
    df = df.withColumn('is_full_time_employee', F.col('is_full_time_employee').cast(T.BooleanType()))
    df = df.join(employee.select('employee_id', 'gps_division'), 'employee_id', 'left')
    df = get_gps_details(df, country)
    df = gps_division(df, 'employee_activity')
    df = one_view_filters(df, 'employee_activity')
    return df


def segmentation(**kwargs):
    segmentation = kwargs['segmentation']
    contact_reason = kwargs['contact_reason']
    iso_country_mapping = kwargs['iso_country_mapping']
    stakeholder = kwargs['stakeholder']
    country = kwargs['country']
    account_contact_reason_territory = kwargs['one_account_to_one_contact_reason_to_many_territory']
    segmentation_jp = kwargs['japan_segmentation']
    COLUMNS_TO_BE_SELECTED_CONTACT_REASON = [
        'contact_reason_id',
        'contact_reason_name',
        'brand',
        'gps_division'
    ]

    COLUMNS_TO_BE_RENAMED_SEGMENTATION = [
        ("country_id", "country_id"),
        ("account_id", "veeva_account_id"),
        ("product_id", "product_id"),
        ("segmentation_type", "segmentation_type"),
        ('segmentation_value', 'segmentation_value'),
        ('adoption_ladder', 'adoption_ladder'),
        ('row_is_current', 'row_is_current'),
        ('row_start_date', 'valid_from_date'),
        ('row_end_date', 'row_end_date'),
        ('segmentation_core_value', 'segmentation_global_value'),
        ('row_last_modified', 'row_last_modified')
    ]

    COLUMNS_TO_BE_SELECTED_SEGMENTATION = [col[1] for col in COLUMNS_TO_BE_RENAMED_SEGMENTATION]

    SELECT_COLS = [
        'country_id',
        'veeva_account_id',
        'row_is_current',
        'valid_from_date',
        'brand',
        'contact_reason_id',
        'segmentation_type',
        'segmentation_value',
        'segmentation_global_value',
        'row_end_date',
        'gps_division',
        'row_last_modified'
    ]

    NO_SEG_CHANGE_SELECT_COLS = [
        'country_id',
        'veeva_account_id',
        'row_is_current',
        'brand',
        'contact_reason_id',
        'segmentation_type',
        'new_segment_value',
        'old_segment_value',
        'segmentation_change_flag',
        'segmentation_global_value',
        'gps_division'
    ]

    FINAL_SELECT_COLS = [
        'segmentation_change_id',
        'country_id',
        'veeva_account_id',
        'contact_reason_id',
        'brand',
        'segmentation_type',
        'current_segment_value',
        'current_segment_id',
        'valid_from_date',
        'valid_end_date',
        'row_is_current',
        'new_segment_value',
        'new_segment_id',
        'old_segment_value',
        'old_segment_id',
        'segmentation_global_value',
        'gps_division',
        'row_last_modified',
        'veeva_territory_id'
    ]
    # https://btsjira.abbvie.com/browse/OFUS-350
    # JPN - these are removed to not have impact to other usecase and this is history data not required for analysis
    removal_filter_2 = ((F.col('country') == 'JPN') & (F.col('source_row') == 'ABV_CORE_PRODUCT_CLASSIFICATION__C'))
    segmentation = segmentation.filter(
        (removal_filter_2 == False)
    )
    df = standardize_country_code(segmentation.withColumnRenamed('country', 'country_id'), iso_country_mapping)
    # removed no segment filter , as its required for analytics
    df = df.filter(
        (F.col('segmentation_value').isNotNull()) &
        (F.col('country_id').isNotNull())
    )
    for columns in COLUMNS_TO_BE_RENAMED_SEGMENTATION:
        df = df.withColumnRenamed(columns[0], columns[1])

    df = df.select(COLUMNS_TO_BE_SELECTED_SEGMENTATION). \
        withColumn('valid_from_date', F.to_date(F.col('valid_from_date'))). \
        withColumn('row_end_date', F.to_date(F.col('row_end_date'))). \
        dropDuplicates()

    # add contact_reason in segmentation
    df = df.withColumn(
        "contact_reason_id",
        F.concat_ws(
            '_',
            *[F.col("country_id"),
              F.col("product_id")]
        )
    )

    # clean the contact_reason_id column
    df = df.withColumn(
        "contact_reason_id",
        F.when(
            F.col('contact_reason_id').contains('_'),
            F.col('contact_reason_id')
        ).otherwise(
            F.lit(None)
        )
    ).drop('product_id')
    # select the columns from contact_reason
    # contact reason columns
    contact_reason = contact_reason.select(
        *COLUMNS_TO_BE_SELECTED_CONTACT_REASON
    ).dropDuplicates()

    df = df.join(
        F.broadcast(contact_reason),
        'contact_reason_id',
        'left'
    )

    df = df.select(SELECT_COLS).distinct()
    df = df.join(account_contact_reason_territory, ['veeva_account_id', 'contact_reason_id'], 'left')
    # Start # Mission : Make up segmetation data of Japan as PRODUCT_METRICS_VOD__C is not used. MCCP has the doctor rank for the HCP at territory level!!! ############
    df = safe_union(segmentation_jp, df)
    # End # Mission : Make up segmetation data of Japan !!! ############
    # update the segmentation type as in veeva
    # Adoption is also Adoption Ladder, since history its called as Adoption
    df = df.withColumn(
        'segmentation_type',
        F.when(
            F.col('segmentation_type').isin(['Adoption Ladder', 'Adoption']),
            F.lit('Adoption Ladder')
        ).otherwise(
            F.when(F.col('segmentation_type') == 'Qualitative Segmentation', F.lit('Customer Segmentation')).otherwise(
                F.when(
                    F.col('segmentation_type').isin(['Quantitative Segmentation', 'Quantitative Segmentation 9 Box']),
                    F.lit('PA Segmentation')).otherwise(
                    F.when(F.col('segmentation_type') == 'Tier Segmentation', F.lit('Tier Segmentation')).otherwise(
                        F.col('segmentation_type')
                    )
                )
            )
        ))
    # segmentation at contact reason(product) level
    brand_window = Window.partitionBy(
        df.country_id,
        df.veeva_account_id,
        df.contact_reason_id,
        df.segmentation_type,
    ).orderBy(
        df.valid_from_date.desc()
    )
    df = df.withColumn(
        'valid_end_date', F.col('row_end_date')
    )
    df = df.withColumn(
        'old_segment_value',
        F.lead('segmentation_value').over(brand_window)
    ).withColumnRenamed(
        'segmentation_value',
        'new_segment_value'
    ).withColumn(
        'segmentation_change_flag',
        F.when(
            (F.col('old_segment_value').isNull()) &
            (F.col('new_segment_value').isNotNull()),
            F.lit('true')
        ).when(
            (F.col('row_is_current')),  # case insensitive
            F.lit('true')
        ).when(
            (F.col('old_segment_value').isNotNull()) &
            (F.col('new_segment_value').isNotNull()) &
            (F.col('old_segment_value') != F.col('new_segment_value')),  # case insensitive
            F.lit('true')
        ).otherwise(
            F.lit('false')
        )
    )

    df = df.withColumn('valid_end_date', F.coalesce('valid_end_date', 'row_end_date'))
    df = df.withColumn(
        'segmentation_change_id',
        F.concat_ws(
            ' - ',
            'country_id',
            'veeva_account_id',
            'brand',
            F.split('contact_reason_id', '_')[1],
            'segmentation_type',
            F.coalesce(F.col('old_segment_value'), F.lit('NO SEGMENT')),
            'new_segment_value',
            'valid_from_date'
        )
    ).withColumn(
        'new_segment_id',
        F.concat_ws(' - ', 'country_id', 'brand', F.split('contact_reason_id', '_')[1], 'segmentation_type',
                    'new_segment_value')
    ).withColumn(
        'old_segment_id',
        F.when(
            F.col('old_segment_value').isNotNull(),
            F.concat_ws(' - ', 'country_id', 'brand', F.split('contact_reason_id', '_')[1], 'segmentation_type',
                        'old_segment_value')
        ).otherwise(
            F.lit(None)
        )
    )
    window_by_brand = Window.partitionBy(
        df.country_id,
        df.veeva_account_id,
        df.contact_reason_id,
        df.segmentation_type,
    ).orderBy(
        df.valid_end_date.desc()
    )

    df = df.withColumn(
        'temp_row_num',
        F.row_number().over(window_by_brand)
    ).withColumn(
        'temp_current_segmentation',
        F.first('new_segment_value').over(window_by_brand)
    )

    df = df.withColumn(
        'current_segment_value',
        F.when(
            F.col('temp_row_num') == 1, F.col('temp_current_segmentation')
        ).otherwise(F.lit(None))
    ).withColumn(
        'current_segment_id',
        F.when(
            F.col('current_segment_value').isNotNull(), F.col('new_segment_id')
        ).otherwise(F.lit(None))
    ).drop(
        'temp_row_num'
    ).drop(
        'temp_current_segmentation'
    )

    # reordering
    df = df.select(
        FINAL_SELECT_COLS
    )
    df = df.withColumnRenamed('veeva_account_id', 'stakeholder_id')
    df = add_in_stakeholder_id(df, stakeholder). \
        withColumnRenamed('source_account_id', 'veeva_account_id').drop('source_account'). \
        withColumn('segmentation_id', F.sha2(
        F.concat_ws('_', *df.columns), 256)).distinct()

    df = gps_division(df, 'segmentation')
    df = get_gps_details(df, country)
    return df.coalesce(2)


def current_segmentation(**kwargs):
    segmentation = kwargs['segmentation']
    contact_reason = kwargs['contact_reason']
    GROUP_BY_COLUMNS = [
        'brand', 'contact_reason_id', 'country_id', 'current_segment_id',
        'current_segment_value',
        'segmentation_change_id', 'new_segment_id', 'new_segment_value',
        'old_segment_id', 'old_segment_value', 'row_is_current', 'segmentation_global_value',
        'segmentation_id', 'segmentation_type', 'stakeholder_id',
        'valid_end_date', 'valid_from_date', 'veeva_account_id', 'affiliate', 'gps_division', 'veeva_territory_id'
    ]
    # filter on only row_is_current
    current_segmentation = segmentation \
        .filter(
        F.col("row_is_current") == True
    )
    current_segmentation = current_segmentation.join(
        contact_reason.select('contact_reason_id', 'indication'),
        'contact_reason_id',
        'left'
    )
    current_segmentation = current_segmentation. \
        groupBy(GROUP_BY_COLUMNS). \
        agg(
        F.collect_set('indication').alias('indication'),
        F.max('row_last_modified').alias('row_last_modified')
    ). \
        dropDuplicates()
    # PA Segmentation allowed to see commercial data
    current_segmentation = current_segmentation.withColumn('division', F.when(
        F.col('segmentation_type') == 'PA Segmentation', F.lit('commercial')).otherwise(F.lit('all')))
    # PK column
    current_segmentation = current_segmentation. \
        withColumn('current_segmentation_id', F.sha2(F.col('segmentation_change_id'), 256))
    return current_segmentation.distinct()


def potential_affinity(**kwargs):
    segmentation = kwargs['segmentation']
    segmentation = segmentation.filter(F.col('segmentation_type') == 'PA Segmentation') \
        .withColumn("segmentation_type", F.lit("potential_affinity"))

    potential_affinity = segmentation \
        .filter(F.col('segmentation_type') == 'potential_affinity') \
        .groupby(
        "country_id",
        "contact_reason_id",
        "veeva_account_id",
        "valid_from_date",
        "valid_end_date",
    ).agg(
        F.first("new_segment_value").alias('potential_affinity'),
        F.first("row_is_current").alias('row_is_current'),
        F.max("row_last_modified").alias('row_last_modified')
    ).dropDuplicates()

    potential_affinity = potential_affinity.withColumn(
        "potential",
        F.substring(
            F.col("potential_affinity"),
            1,
            1
        )
    ).withColumn(
        "affinity",
        F.substring(
            F.col("potential_affinity"),
            2,
            1
        )
    )
    return potential_affinity


def customer_segmentation(**kwargs):
    segmentation = kwargs['segmentation']
    segmentation = segmentation.filter(F.col('segmentation_type') == 'Customer Segmentation') \
        .withColumn("segmentation_type", F.lit("customer_segmentation"))
    customer_segmentation = segmentation \
        .filter(F.col('segmentation_type') == 'customer_segmentation') \
        .groupby(
        "country_id",
        "contact_reason_id",
        "veeva_account_id",
        "valid_from_date",
        "valid_end_date",
    ).agg(
        F.first("new_segment_value").alias('customer_segmentation'),
        F.first("row_is_current").alias('row_is_current'),
        F.max("row_last_modified").alias('row_last_modified')
    ).dropDuplicates()
    return customer_segmentation


def adoption_ladder(**kwargs):
    segmentation = kwargs['segmentation']
    segmentation = segmentation.filter(F.col('segmentation_type') == 'Adoption Ladder') \
        .withColumn("segmentation_type", F.lit("adoption_ladder"))

    adoption_ladder = segmentation \
        .groupby(
        "country_id",
        "contact_reason_id",
        "veeva_account_id",
        "valid_from_date",
        "valid_end_date",
    ).agg(
        F.first("new_segment_value").alias('adoption_ladder'),
        F.first("row_is_current").alias("row_is_current"),
        F.max("row_last_modified").alias("row_last_modified")
    ).dropDuplicates()
    return adoption_ladder


def tier_segmentation(**kwargs):
    segmentation = kwargs['segmentation']
    segmentation = segmentation.filter(F.col('segmentation_type') == 'Tier Segmentation') \
        .withColumn("segmentation_type", F.lit("tier_segmentation"))
    tier_segmentation = segmentation \
        .groupby(
        "country_id",
        "contact_reason_id",
        "veeva_account_id",
        "valid_from_date",
        "valid_end_date",
    ).agg(
        F.first("new_segment_value").alias('tier_segmentation'),
        F.first("row_is_current").alias("row_is_current"),
        F.max("row_last_modified").alias("row_last_modified")
    ).dropDuplicates()
    return tier_segmentation


def key_message(**kwargs):
    key_message = kwargs['key_message']
    iso_country_mapping = kwargs['iso_country_mapping']
    local_key_messages = kwargs['local_key_messages']
    global_key_messages = kwargs['global_key_messages']
    country = kwargs['country']
    contact_reason = kwargs['contact_reason']
    COLUMNS_TO_BE_RENAMED = [
        ("id", "key_message_id"),
        ("name", "name"),
        ("category", "category"),
        ("country", "country_id"),
        ("create_date", "creation_date"),
        ("description", "description"),
        ("is_active", "is_active"),
        ("last_modified_date", "last_modification_date"),
        ("product_id", "product_id"),
        ("status", "status"),
        ("source", "source"),
        ("audience_of_content", "audience_of_content"),
        ("local_key_message", "local_key_message"),  # used in contacts
        ('vault_doc_id', 'vault_document_id'),
        ('vault_dns', 'vault_instance_id'),
        ('language', 'language'),
        ('core_category', 'core_category'),
        ("closed_loop_marketing_id", "clm_code"),
        ("global_key_message_category", "global_key_message_category"),
        ('global_key_message_vault_id', 'content_vault_id_global'),
        ('local_key_message_vault_id', 'content_vault_id_local'),
        ('row_is_current', 'row_is_current'),
        ('row_last_modified', 'row_last_modified')
    ]

    COLUMNS_TO_BE_SELECTED = [
        "{column}".format(column=columns[1]) for columns in COLUMNS_TO_BE_RENAMED
    ]

    COLUMNS_TO_BE_SELECTED_LKM = [
        'id',
        'global_key_message_id',
        'local_market_value_driver',
        'global_market_value_driver',
        'is_active',
        'global_key_message'
    ]

    COLUMNS_TO_BE_SELECTED_LKM_FL = [
        'id',
        'global_key_message_id',
        'country',
        'category',
        'global_key_message',
        'local_market_value_driver',
        'global_market_value_driver',
        'is_active',
        'created_date',
        'last_modified_date',
        'row_is_current',
        'row_last_modified'
    ]

    COLUMNS_TO_BE_SELECTED_GKM = [
        'id',
        'global_market_value_driver'
    ]
    # Rename columns
    for columns in COLUMNS_TO_BE_RENAMED:
        key_message = key_message.withColumnRenamed(columns[0], columns[1])
    key_message = key_message.filter(F.col('row_is_current') == True). \
        withColumn('country_id', F.coalesce('country_id', 'core_flag'))
    key_message = key_message.select(*COLUMNS_TO_BE_SELECTED).drop_duplicates()

    lkm = (
        local_key_messages
            .select("global_key_message_id",
                    F.col("global_key_message_name").alias("tagged_global_key_message_name"))
            .dropDuplicates()
    )
    local_key_messages_FL = local_key_messages.select(*COLUMNS_TO_BE_SELECTED_LKM_FL).dropDuplicates()

    local_key_messages = local_key_messages.select(*COLUMNS_TO_BE_SELECTED_LKM).dropDuplicates()
    global_key_messages = global_key_messages.select(*COLUMNS_TO_BE_SELECTED_GKM).dropDuplicates()

    local_key_messages = local_key_messages.withColumnRenamed('global_market_value_driver',
                                                              'global_market_value_driver_lkm') \
        .withColumnRenamed('is_active', 'is_active_local') \
        .withColumnRenamed('global_key_message', 'content_name_global')

    local_key_messages_FL = local_key_messages_FL.withColumnRenamed('global_market_value_driver',
                                                                    'global_market_value_driver_lkm') \
        .withColumnRenamed('is_active', 'is_active_local') \
        .withColumnRenamed('global_key_message', 'content_name_global') \
        .withColumnRenamed('created_date', 'creation_date') \
        .withColumnRenamed('last_modified_date', 'last_modification_date') \
        .withColumnRenamed('row_is_current', 'row_is_current_local') \
        .withColumnRenamed('row_last_modified', 'row_last_modified_local')

    local_key_messages_FL = local_key_messages_FL.join(iso_country_mapping, [
        F.upper(local_key_messages_FL.country) == F.upper(iso_country_mapping.country_name)
        ],
                                                       'left').drop(F.col('google_name')).drop(
        F.col('country_name')).drop(F.col('ISO2')) \
        .drop(F.col('country'))
    local_key_messages_FL = local_key_messages_FL.withColumnRenamed('ISO3', 'country_id')
    # standardize country column
    key_message = standardize_country_code(key_message, iso_country_mapping)

    key_message = key_message.withColumn(
        "contact_reason_id",
        F.concat_ws(
            '_',
            *[F.col("country_id"),
              F.col("product_id")]
        )
    )

    # clean the contact_reason_id column
    key_message = key_message.withColumn(
        "contact_reason_id",
        F.when(
            F.col('contact_reason_id').contains('_'),
            F.col('contact_reason_id')
        ).otherwise(
            F.lit(None)
        )
    ).drop('product_id')
    key_message = key_message.join(contact_reason.select('contact_reason_id', 'gps_division'), 'contact_reason_id',
                                   'left')

    key_message_veeva = key_message.join(local_key_messages,
                                         [key_message.content_vault_id_local == local_key_messages.id,
                                          key_message.content_vault_id_global == local_key_messages.global_key_message_id],
                                         'left').drop(F.col('id')).drop(F.col('global_key_message_id'))

    key_message = key_message.drop(F.col('country_id')).drop(F.col('category')) \
        .drop(F.col('creation_date')).drop(F.col('last_modification_date'))

    key_message_vault = key_message.join(local_key_messages_FL,
                                         [key_message.content_vault_id_local == local_key_messages.id,
                                          key_message.content_vault_id_global == local_key_messages.global_key_message_id],
                                         'right').filter(F.col('content_vault_id_local').isNull() &
                                                         F.col('content_vault_id_global').isNull() &
                                                         F.col('id').isNotNull() & F.col(
        'global_key_message_id').isNotNull())

    key_message_vault = key_message_vault.drop(F.col('content_vault_id_local')).drop(F.col('content_vault_id_global'))
    key_message_vault = key_message_vault.drop(F.col('row_is_current')).drop(F.col('row_last_modified'))
    key_message_vault = key_message_vault.withColumnRenamed('id', 'content_vault_id_local') \
        .withColumnRenamed('global_key_message_id', 'content_vault_id_global') \
        .withColumnRenamed('row_is_current_local', 'row_is_current') \
        .withColumnRenamed('row_last_modified_local', 'row_last_modified')
    key_message_vault = key_message_vault.withColumn('key_message_id', F.concat_ws(
        '_',
        *[F.col("content_vault_id_local"),
          F.col("content_vault_id_global")]
    ))

    key_message = key_message_veeva.unionByName(key_message_vault)

    key_message = key_message.join(global_key_messages, [key_message.content_vault_id_global == global_key_messages.id],
                                   'left').drop(F.col('id'))

    key_message = key_message.withColumn('global_market_value_driver', F.coalesce('global_market_value_driver_lkm',
                                                                                  'global_market_value_driver')) \
        .drop(F.col('global_market_value_driver_lkm')).drop_duplicates()

    # Add global tagging information
    key_message = key_message.withColumn('global_message_tagged', F.when(F.col('content_vault_id_global').isNull()
                                                                         , F.lit('false'))
                                         .otherwise(F.lit('true')).cast('boolean'))

    key_message = key_message.withColumn('category',
                                         F.coalesce(F.col("category"), F.lit('No data available from source'))) \
        .withColumn('core_category', F.coalesce(F.col("core_category"), F.lit('No data available from source'))) \
        .withColumn('global_key_message_category',
                    F.coalesce(F.col("global_key_message_category"), F.lit('No data available from source')))

    key_message = gps_division(key_message, 'content')
    return get_gps_details(key_message, country)


def content(**kwargs):
    key_messages = kwargs['key_messages']
    calls_discussions = kwargs['calls_discussions']
    calls_key_messages = kwargs['calls_key_messages']
    iso_country_mapping = kwargs['iso_country_mapping']
    local_key_messages = kwargs['local_key_messages']
    country = kwargs['country'].repartition(1)
    contact_reason = kwargs['contact_reason']
    COLUMNS_TO_BE_RENAMED_KEY_MESSAGES = [
        ("id", "key_message_id"),
        ("name", "content_name"),
        ("description", "content_description"),
        ("category", "content_category"),
        # available in vault data , its same as key_message_category, category in clean/local_key_messages
        ("country", "country_id"),
        ("product_id", "product_id"),
        ("status", "status"),
        ("source", "source"),
        ("audience_of_content", "audience"),
        ("is_active", "is_active"),
        ("create_date", "creation_date"),
        ("last_modified_date", "last_modified_date"),
        ('vault_doc_id', 'vault_document_id'),
        ('vault_dns', 'vault_instance_id'),
        ("local_key_message", "content_name_local"),
        ("local_key_message_vault_id", "content_vault_id_local"),
        ("global_key_message", "content_name_global"),
        ("global_key_message_category", "content_category_global"),
        ("global_key_message_vault_id", "content_vault_id_global"),
        ("is_global_approved", "is_global_approved"),
        ("media_filename", "media_filename"),
        ("core_category", "core_category"),  # not available in vault data
        ('row_is_current', 'row_is_current'),
        ('row_last_modified', 'row_last_modified'),
        ("closed_loop_marketing_id", "closed_loop_marketing_id")
    ]

    COLUMNS_TO_BE_SELECTED_KEY_MESSAGES = [col[1] for col in COLUMNS_TO_BE_RENAMED_KEY_MESSAGES]

    # ******************* DISCUSSION TOPIC ********************** #

    COLUMNS_TO_BE_RENAMED_DISCUSSION_TOPIC = [
        ("discussion_topic_english", "content_name"),
        ("country", "country_id"),
        ("product_id", "product_id"),
        ("interaction_topic_english", "classification"),
        ("row_is_current", "row_is_current"),
        ("row_last_modified", "row_last_modified"),
        ("interaction_topic_id", "interaction_topic_id")

    ]

    COLUMNS_TO_BE_SELECTED_DISCUSSION_TOPIC = [col[1] for col in COLUMNS_TO_BE_RENAMED_DISCUSSION_TOPIC]

    COLUMNS_TO_BE_RENAMED_VAULT_LOCAL_KEY_MESSAGES = [
        ("description", "content_description"),
        ("category", "content_category"),  # key_message_category, category in clean/local_key_messages are always same
        ("country", "country_name"),
        ("product", "contact_reason_name"),
        ("status", "status"),
        ("is_active", "is_active"),
        ("created_date", "creation_date"),
        ("last_modified_date", "last_modified_date"),
        ("local_key_message", "content_name_local"),
        ("id", "content_vault_id_local"),
        ("global_key_message", "content_name_global"),
        ("key_message_category", "content_category_global"),
        ("global_key_message_id", "content_vault_id_global"),
        ("is_vault_only", "is_vault_only"),
        ('row_is_current', 'row_is_current'),
        ('row_last_modified', 'row_last_modified')
    ]

    # -------------------- DISCUSSION TOPIC ------------------------ #

    # Temp version of calls_discussions
    discussion_topic = calls_discussions.filter(F.col('row_is_current') == True)

    # Renaming columns in order to be merged
    for columns in COLUMNS_TO_BE_RENAMED_DISCUSSION_TOPIC:
        discussion_topic = discussion_topic.withColumnRenamed(columns[0], columns[1])

    # Select only a subset of columns
    discussion_topic = discussion_topic.select(
        *COLUMNS_TO_BE_SELECTED_DISCUSSION_TOPIC) \
        .dropDuplicates()

    # Add the content type column
    discussion_topic = discussion_topic.withColumn(
        "content_type",
        F.lit("Discussion Topic")
    )
    discussion_topic = discussion_topic.withColumn(
        'classification', F.coalesce('classification', F.lit('No interaction topic'))
    ).withColumn(
        'content_name', F.coalesce('content_name', F.lit('No content discussed'))
    ).withColumn(
        'interaction_topic_id', F.coalesce('interaction_topic_id', F.lit(''))
    )
    # -----------------------  KEY MESSAGES -------------------------- #
    key_messages = key_messages.filter(F.col('row_is_current') == True). \
        withColumn('country', F.coalesce('country', 'core_flag'))

    # Adding key_message_id from calls_key_messages to ensure all content ids are available ,for details refer OFUS-177
    calls_key_messages = calls_key_messages.select("country", "key_message_id").dropDuplicates()
    calls_key_messages = calls_key_messages.filter(F.col("country").isNotNull())
    calls_key_messages = calls_key_messages.withColumnRenamed("country",
                                                              "key_message_delivered_country").withColumnRenamed(
        "key_message_id", "id")
    calls_key_messages = calls_key_messages.withColumn(
        "key_message_delivered_country",
        F.concat_ws(';', F.collect_list(F.col('key_message_delivered_country')).over(Window.partitionBy('id')))
    ).dropDuplicates()
    key_messages = key_messages.join(calls_key_messages, "id", "left")

    key_messages = key_messages.withColumn('country', F.when(
        F.col('key_message_delivered_country').isNotNull(), F.concat_ws(';', 'country', 'key_message_delivered_country')
    ).otherwise(F.col('country')))
    # country has multiple values
    key_messages = key_messages.withColumn('country', F.explode(F.split('country', ';')))
    key_messages = key_messages.withColumn('country', F.explode(F.split('country', ',')))

    # Renaming columns in order to be merged
    for columns in COLUMNS_TO_BE_RENAMED_KEY_MESSAGES:
        key_messages = key_messages.withColumnRenamed(columns[0], columns[1])

    # Select only a subset of columns
    key_messages = key_messages.select(
        *COLUMNS_TO_BE_SELECTED_KEY_MESSAGES) \
        .dropDuplicates()
    key_messages = standardize_country_code(key_messages, iso_country_mapping)
    # Add the content type column
    key_messages = key_messages.withColumn(
        "content_type",
        F.lit("Key Message")
    )

    # Concatenate the country id and country type in front of the key message, too
    key_messages = key_messages.withColumn(
        "content_id",
        F.concat_ws(
            "_",
            F.coalesce(
                F.col("country_id"),
                F.lit("")
            ),
            F.coalesce(
                F.col("content_type"),
                F.lit("")
            ),
            F.coalesce(
                F.col("key_message_id"),
                F.lit("")
            )
        )
    ).dropDuplicates()
    # cleaning the data
    key_messages = key_messages.withColumn("content_name", F.regexp_replace('content_name', "  ", " "))
    # Add global tagging information
    lkm = local_key_messages.select(F.col('id').alias('content_vault_id_local'), 'local_market_value_driver',
                                    'global_market_value_driver')
    key_messages = (
        key_messages
            .join(lkm, 'content_vault_id_local', "left") \
            .withColumn('is_global_message_tagged', F.when(
            F.col('content_vault_id_global').isNotNull(), F.lit(True)).otherwise(F.lit(False)))
    )
    key_messages = key_messages.withColumn('core_category',
                                           F.coalesce(F.col("core_category"), F.lit('No data available from source')))

    for columns in COLUMNS_TO_BE_RENAMED_VAULT_LOCAL_KEY_MESSAGES:
        local_key_messages = local_key_messages.withColumnRenamed(columns[0], columns[1])

    COLUMNS_TO_BE_SELECTED_LKM = [
        "country_name",
        "contact_reason_name",
        "is_active",
        "creation_date",
        "last_modified_date",
        "content_name_local",
        "content_vault_id_local",
        "content_name_global",
        "content_category_global",
        "content_vault_id_global",
        "is_vault_only",
        'local_market_value_driver',
        'global_market_value_driver',
    ]
    # Select only a subset of columns
    local_key_messages = local_key_messages.select(
        *COLUMNS_TO_BE_SELECTED_LKM)
    local_key_messages = local_key_messages.join(country.select('country_id', 'country_name'), 'country_name', 'left') \
        .drop('country_name')
    # these records aims at country == MASTER
    master_local_key_messages = local_key_messages.filter(F.col('country_id').isNull())

    # Rinvoq_RA , BEL_a0007000001lxuqAAA , BEL_a001v00002KP02YAAT
    contact_reason = contact_reason.select('contact_reason_id', 'contact_reason_name', 'country_id', 'gps_division')
    update_window = Window.partitionBy(
        ['country_id', 'contact_reason_name', 'gps_division']).orderBy('contact_reason_name')
    contact_reason = contact_reason.withColumn(
        "contact_reason_id", F.collect_set(F.col('contact_reason_id')).over(update_window)[0]
    ).dropDuplicates()
    master_local_key_messages = master_local_key_messages.drop('country_id').join(contact_reason, 'contact_reason_name',
                                                                                  'left')
    master_local_key_messages = master_local_key_messages.withColumn('is_global', F.lit(True))
    local_key_messages = local_key_messages.filter(F.col('country_id').isNotNull())
    local_key_messages = local_key_messages.join(contact_reason, ['contact_reason_name', 'country_id'], 'left')
    local_key_messages = local_key_messages.withColumn('is_global', F.lit(False))
    vault_key_message = safe_union(master_local_key_messages, local_key_messages)
    # Add the content type column
    vault_key_message = vault_key_message.withColumn(
        "content_type",
        F.lit("Local Key Message")
    )
    # Concatenate the country id and country type in front of the key message, too
    vault_key_message = vault_key_message.withColumn(
        "content_id",
        F.concat_ws(
            "_",
            F.coalesce(
                F.col("country_id"),
                F.lit("")
            ),
            F.coalesce(
                F.col("content_type"),
                F.lit("")
            ),
            F.coalesce(
                F.col("content_vault_id_local"),
                F.lit("")
            )
        )
    )
    vault_key_message = vault_key_message.withColumn('content_name', F.col('content_name_local')).drop('country_name',
                                                                                                       'contact_reason_name') \
        .withColumn('content_category', F.col('content_category_global')) \
        .withColumn('is_global_message_tagged', F.when(
        F.col('content_vault_id_global').isNotNull(), F.lit(True)).otherwise(F.lit(False))) \
        .withColumn('from_source', F.lit('vault'))
    key_messages = key_messages.withColumn('from_source', F.lit('veeva'))
    key_messages = safe_union(vault_key_message, key_messages).drop('gps_division')
    key_messages = key_messages.withColumn('content_category', F.coalesce(F.col("content_category"),
                                                                          F.lit('No data available from source'))) \
        .withColumn('content_category_global',
                    F.coalesce(F.col("content_category_global"), F.lit('No data available from source')))
    # content type - Discussion topic
    # Add a new unique ID for the content, but not for the key messages
    discussion_topic = discussion_topic.withColumn('from_source', F.lit('veeva'))

    content = discussion_topic.withColumn(
        "content_id",
        F.concat_ws(
            "_",
            F.coalesce(
                F.col("country_id"),
                F.lit("")
            ),
            F.coalesce(
                F.col("content_type"),
                F.lit("")
            ),
            F.coalesce(
                F.col("product_id"),
                F.lit("")
            ),
            F.coalesce(
                F.col("content_name"),
                F.lit("")
            ),
            F.concat(
                F.coalesce(F.col("classification"), F.lit("")),
                F.coalesce(F.col("interaction_topic_id"), F.lit(""))
            ),
        )
    )

    content = safe_union(content, key_messages)
    content = standardize_country_code(content, iso_country_mapping)
    content = content \
        .withColumn(
        'contact_reason_id', F.when(
            F.col('from_source') != 'vault', F.concat_ws('_', *[F.col("country_id"), F.col("product_id")])
        ).otherwise(F.col('contact_reason_id'))
    ).drop('product_id')
    # clean the contact_reason_id column
    content = content.withColumn(
        "contact_reason_id",
        F.when(
            F.col('contact_reason_id').contains('_'),
            F.col('contact_reason_id')
        ).otherwise(
            F.lit(None)
        )
    )

    content = content.join(contact_reason.select('contact_reason_id', 'gps_division'), 'contact_reason_id', 'left')
    content = gps_division(content, 'content')
    content = add_cdc_cols(content, 'content')
    content = one_view_filters(content, 'content')
    return get_gps_details(content, country)


def presentation(**kwargs):
    presentation = kwargs['presentation']
    country = kwargs['country']
    contact_reason = kwargs['contact_reason']
    COLUMNS_TO_BE_RENAMED_PRESENTATION = [
        ("id", "id"),
        ("name", "name"),
        ("presentation_id", "title"),
        ("description", "description"),
        ("product_id", "product_id"),
        ("country", "country_id"),
        ("status", "status"),
        ("vaultdocid", "vault_document_id"),
        ("vaultexternalid", "vault_instance_id"),
        ("language", "language"),
        ("source", "source"),
        ("version", "version"),
        ("audience_of_content", "audience"),
        ("end_date", "end_date"),
        ("start_date", "start_date"),
        ("type", "type"),
        ("row_is_current", "row_is_current"),
        ("row_last_modified", "row_last_modified")
    ]

    COLUMNS_TO_BE_SELECTED_PRESENTATION = [col[1] for col in COLUMNS_TO_BE_RENAMED_PRESENTATION]
    # Renaming columns in order to be merged
    for columns in COLUMNS_TO_BE_RENAMED_PRESENTATION:
        presentation = presentation.withColumnRenamed(columns[0], columns[1])
    presentation = presentation.select(
        *COLUMNS_TO_BE_SELECTED_PRESENTATION
    ).dropDuplicates()

    # add contact_reason_id
    presentation = presentation.withColumn(
        "contact_reason_id",
        F.concat_ws(
            '_',
            *[F.col("country_id"),
              F.col("product_id")]
        )
    )

    # clean the contact_reason_id column
    presentation = presentation.withColumn(
        "contact_reason_id",
        F.when(
            F.col('contact_reason_id').contains('_'),
            F.col('contact_reason_id')
        ).otherwise(
            F.lit(None)
        )
    ).withColumnRenamed('id', 'presentation_id'). \
        drop('product_id')

    presentation = add_cdc_cols(presentation, 'presentation')
    presentation = presentation \
        .join(
        contact_reason.select('contact_reason_id', 'gps_division'),
        'contact_reason_id',
        'left')
    presentation = gps_division(presentation, 'presentation')
    return get_gps_details(presentation, country)


def sample_tracking(**kwargs):
    sample_tracking = kwargs['sample_tracking']
    stakeholder = kwargs['stakeholder']
    sample_request_tracker = kwargs['sample_request_tracker']
    country = kwargs['country']
    COLUMNS_TO_BE_RENAMED_SAMPLE_TRACKING = [
        ("account_id", "stakeholder_id"),
        ("amount", "amount"),
        ("attendee_type", "attendee_type"),
        ("call_date", "contact_date"),
        ("call_id", "contact_id"),
        ("country", "country_id"),
        ("create_date", "create_date"),
        ("created_by_id", "employee_id"),
        ("disbursemen_ttype", "disbursement_type"),
        ("id", "sample_tracking_id"),
        ("is_apply_limit", "is_apply_limit"),
        ("is_limit_applied", "is_limit_applied"),
        ("is_parent_call", "is_parent_call"),
        ("lot", "lot_number"),
        ("mobile_id", "mobile_id"),
        ("name", "name"),
        ("parent_product", "parent_contact_reason_name"),
        ("product_id", "contact_reason_id"),
        ("quantity", "delivered_quantity"),
        ("row_is_current", "row_is_current"),
        ("row_last_modified", "row_last_modified")

    ]
    COLUMNS_TO_BE_SELECTED_SAMPLE_TRACKING = [col[1] for col in COLUMNS_TO_BE_RENAMED_SAMPLE_TRACKING]

    COLUMNS_TO_BE_RENAMED_SAMPLE_REQUEST_TRACKER = [
        ("call_sample_id", "sample_tracking_id"),
        ("approved_quantity", "approved_quantity"),
        ("requested_quantity", "requested_quantity"),
        ("comment_manager", "comment_manager"),
        ("comment_representive", "comment_representive"),
        ("date_of_delivery", "date_of_delivery"),
        ("confirm_address_and_send_to_manager", "confirm_address_and_send_to_manager")
    ]

    COLUMNS_TO_BE_SELECTED_SAMPLE_REQUEST_TRACKER = [col[1] for col in COLUMNS_TO_BE_RENAMED_SAMPLE_REQUEST_TRACKER]

    sample_tracking = sample_tracking.filter(F.col('row_is_current') == True)
    # Renaming columns in order to be merged
    for columns in COLUMNS_TO_BE_RENAMED_SAMPLE_TRACKING:
        sample_tracking = sample_tracking.withColumnRenamed(columns[0], columns[1])

    # Select only a subset of columns
    sample_tracking = sample_tracking. \
        select(*COLUMNS_TO_BE_SELECTED_SAMPLE_TRACKING). \
        filter(F.col('country_id').isNotNull()). \
        withColumn('veeva_account_id', F.col('stakeholder_id')). \
        withColumn(
        "contact_reason_id",
        F.concat_ws(
            '_',
            *[F.col("country_id"),
              F.col("contact_reason_id")]
        )
    )
    sample_tracking = add_in_stakeholder_id(sample_tracking, stakeholder). \
        drop('source_account', 'source_account_id')

    sample_request_tracker = sample_request_tracker.filter(F.col('row_is_current') == True)
    # Renaming columns in order to be merged
    for columns in COLUMNS_TO_BE_RENAMED_SAMPLE_REQUEST_TRACKER:
        sample_request_tracker = sample_request_tracker.withColumnRenamed(columns[0], columns[1])

    # Select only a subset of columns
    sample_request_tracker = sample_request_tracker. \
        select(*COLUMNS_TO_BE_SELECTED_SAMPLE_REQUEST_TRACKER)

    sample_tracking = sample_tracking.join(sample_request_tracker, 'sample_tracking_id', 'left')
    sample_tracking = gps_division(sample_tracking, 'sample_tracking')
    return get_gps_details(sample_tracking, country)


def coaching(**kwargs):
    coaching = kwargs['coaching']
    record_type = kwargs['record_type']
    country = kwargs['country']
    COLUMNS_TO_BE_RENAMED_COACHING = [
        ("businessunit", "business_unit"),
        ("comments", "comments"),
        ("country", "country_id"),
        ("create_date", "create_date"),
        ("created_by_id", "created_by_id"),
        ("current_guidelines", "is_following_current_guidelines"),  # converting yes, NO to boolean
        ("duration", "duration"),
        ("employee_id", "employee_id"),
        ("ethical_rules_respect", "ethical_rules_respect"),
        ("id", "coaching_id"),
        ("information_quality_delivered", "is_quality_information_delivered"),  # converting yes, NO to boolean
        ("interventional_studies_follow_up", "is_interventional_studies_follow_up"),  # converting yes, NO to boolean
        ("last_modified_date", "last_modified_date"),
        ("manager", "manager_id"),
        ("name", "coaching_name"),
        ("no_donation_gifts_and_samples", "is_gifts_and_samples_donated"),  # boolean
        ("objective", "objective"),
        ("owner_id", "owner_id"),
        ("owner_language", "owner_language"),
        ("record_type_id", "record_type_id"),
        ("review_date", "review_date"),
        ("status", "status"),
        ("type", "coaching_type"),
        ("row_is_current", "row_is_current"),
        ("row_last_modified", "row_last_modified")
    ]
    COLUMNS_TO_BE_SELECTED_COACHING = [col[1] for col in COLUMNS_TO_BE_RENAMED_COACHING]
    COLUMNS_TO_CONVERT_BOOLEAN_TYPE = ['is_following_current_guidelines', 'is_quality_information_delivered',
                                       'is_interventional_studies_follow_up']

    # remove records that are not present in veeva
    coaching = coaching.filter(F.col('row_is_current') == True)
    for columns in COLUMNS_TO_BE_RENAMED_COACHING:
        coaching = coaching.withColumnRenamed(columns[0], columns[1])
    coaching = coaching.select(COLUMNS_TO_BE_SELECTED_COACHING)
    # transform columns
    coaching = coaching.withColumn('duration', F.coalesce(F.col('duration'), F.lit('no value')))
    for columns in COLUMNS_TO_CONVERT_BOOLEAN_TYPE:
        coaching = coaching.withColumn(columns, F.when(
            F.upper(F.col(columns)) == 'YES', F.lit(True)
        ).otherwise(F.lit(False))
                                       )
    # reverse logic as column is no_donation_gifts_and_samples
    coaching = coaching.withColumn('is_gifts_and_samples_donated', F.when(
        F.upper(F.col('is_gifts_and_samples_donated')) == 'NO', F.lit(True)
    ).otherwise(F.lit(False))
                                   )
    # add record type
    coaching = get_record_type_details(record_type, coaching)
    coaching = gps_division(coaching, 'coaching')
    coaching = add_cdc_cols(coaching, 'coaching')
    return get_gps_details(coaching, country)


def consent(**kwargs):
    multichannel_consent = kwargs['multichannel_consent']
    stakeholder = kwargs['stakeholder']
    record_type = kwargs['record_type']
    country = kwargs['country']
    COLUMNS_TO_BE_RENAMED_CONSENT = [
        ("abv_notification_date", "abv_notification_date"),
        ("account_id", "stakeholder_id"),
        ("capturedatetime", "capturedatetime"),
        ("channel_value", "channel_value"),
        ("consent_line", "consent_line"),
        ("consent_type", "consent_type"),
        ("country_id", "country_id"),
        ("create_date", "create_date"),
        ("created_by_id", "created_by_id"),
        ("default_consent_text", "default_consent_text"),
        ("disclaimer_text", "disclaimer_text"),
        ("id", "consent_id"),
        ("is_active", "is_active"),
        ("is_mail_sent", "is_mail_sent"),
        ("is_mcc_attachment", "is_mcc_attachment"),
        ("last_device", "last_device"),
        ("mail_sent_date", "mail_sent_date"),
        ("mobile_id", "mobile_id"),
        ("name", "consent_name"),
        ("opt_experation_date", "opt_experation_date"),
        ("opt_type", "opt_type"),
        ("product_id", "contact_reason_id"),
        ("record_type_id", "record_type_id"),
        ("row_is_current", "row_is_current"),
        ("signature", "signature"),
        ("signature_datetime", "signature_datetime"),
        ("signature_id", "signature_id"),
        ("sub_channel", "sub_channel"),
        ('veeva_account_id', 'veeva_account_id'),
        ('row_last_modified', 'row_last_modified')
    ]
    COLUMNS_TO_BE_SELECTED_CONSENT = [col[1] for col in COLUMNS_TO_BE_RENAMED_CONSENT]
    # temp fix , to be removed when country cleanup in place.
    country = country.withColumnRenamed('country_id', 'ISO3'). \
        withColumnRenamed('iso2', 'ISO2')

    multichannel_consent = multichannel_consent.filter(F.col('row_is_current') == True). \
        withColumn('veeva_account_id', F.col("account_id"))
    df = standardize_country_code(multichannel_consent.withColumnRenamed('country', 'country_id'), country)
    # ------------- CONSENT  ---------------------#
    # select columns from employee to map country
    # Renaming columns in order to be merged
    for columns in COLUMNS_TO_BE_RENAMED_CONSENT:
        df = df.withColumnRenamed(columns[0], columns[1])
    # Standardise the contact reason ID column via addition of countries
    df = df \
        .withColumn(
        "contact_reason_id", F.when(
            F.col('contact_reason_id').isNotNull(), F.concat_ws('_', *[F.col("country_id"), F.col("contact_reason_id")])
        ).otherwise(F.col('contact_reason_id'))
    )
    df = df.select(
        COLUMNS_TO_BE_SELECTED_CONSENT
    ).dropDuplicates()
    df = add_in_stakeholder_id(df, stakeholder)

    df = df.drop('source_account', 'source_account_id')
    df = get_record_type_details(record_type, df)
    df = get_gps_details(df, country.withColumnRenamed('ISO3', 'country_id'))
    df = gps_division(df, 'consent')
    return df


def calls_click_stream(**kwargs):
    calls_click_streams = kwargs['calls_click_streams']
    calls = kwargs['calls']
    key_message = kwargs['key_message']
    presentation = kwargs['presentation']
    key_message_to_presentation_slide = kwargs['key_message_to_presentation_slide']
    contact_reason = kwargs['contact_reason']
    iso_country_mapping = kwargs['iso_country_mapping']
    country = kwargs['country']
    COLUMNS_TO_BE_RENAMED_CALLS = [
        ("id", "contact_id")
    ]

    COLUMNS_TO_BE_SELECTED_CALLS = [col[1] for col in COLUMNS_TO_BE_RENAMED_CALLS]

    COLUMNS_TO_BE_RENAMED_CONTACT_REASON = [
        ('contact_reason_id', 'contact_reason_id'),
        ('provider_id', 'contact_reason_code')
    ]

    COLUMNS_TO_BE_SELECTED_CONTACT_REASON = [col[1] for col in COLUMNS_TO_BE_RENAMED_CONTACT_REASON]

    # Apply the country code standardisation a100Y000002aOTXQA2
    # calls_click_streams = standardize_country_code(calls_click_streams, iso_country_mapping)
    calls_click_streams = calls_click_streams.withColumnRenamed('country', 'country_id')

    calls_click_streams = calls_click_streams. \
        withColumn('PosAnsSplit', F.split(F.coalesce(F.when(
        F.col("possible_answers").isNotNull(), F.col('possible_answers')).otherwise(
        F.lit(0)), F.lit(0)), ",")
                   )

    calls_click_streams = calls_click_streams.withColumn('PosAnsSplit', F.explode('PosAnsSplit')). \
        filter((F.col('PosAnsSplit') != '') & F.col('row_is_current') == True)

    calls_click_streams = calls_click_streams.select('country_id',
                                                     'question',
                                                     'answer',
                                                     'is_popup_opened',
                                                     F.col('PosAnsSplit').alias('possible_answers'),
                                                     F.col('call_id').alias('contact_id'),
                                                     'key_message_id',
                                                     F.col('id').alias('event_click_stream_code'),
                                                     'track_element_type',
                                                     'track_element_description',
                                                     F.col('product_id').alias('veeva_product_id'),
                                                     'row_is_current',
                                                     'row_last_modified'
                                                     )
    # records deleted from veeva are not considered
    calls = calls.filter(F.col('row_is_current') == True)
    # Renaming columns in order to be merged
    for columns in COLUMNS_TO_BE_RENAMED_CALLS:
        calls = calls.withColumnRenamed(columns[0], columns[1])

    # Select only a subset of columns
    calls = calls.select(
        *COLUMNS_TO_BE_SELECTED_CALLS)

    # Make sure there are no duplicates
    calls = calls.dropDuplicates()

    calls_click_streams = calls_click_streams.join(calls, 'contact_id')

    key_message = key_message.filter((F.col('source') == 'veeva') & (F.col('content_type') == 'Key Message')).select(
        'key_message_id',
        F.col('content_name').alias('key_message_name'),
        F.col('content_description').alias('key_message_desc')
    ).distinct()

    key_message_to_presentation_slide = key_message_to_presentation_slide. \
        filter(F.col('row_is_current') == True). \
        select(
        'presentation_id',
        'key_message_id'
    ).distinct()

    key_message = key_message.join(
        key_message_to_presentation_slide,
        'key_message_id',
        'left'
    )
    presentation = presentation.filter(F.col('row_is_current') == True)
    presentation = presentation.select(F.col('presentation_id'), F.col('name').alias('presentation_name'))

    key_message = key_message.join(presentation, ['presentation_id'], 'left')

    calls_click_streams = calls_click_streams.join(key_message, ['key_message_id'], 'left')

    # According to the new conceptual differentiation of contact reason vs product, rename the column
    calls_click_streams = calls_click_streams \
        .withColumn(
        "contact_reason_id",
        F.concat_ws(
            '_',
            *[F.col("country_id"),
              F.col("veeva_product_id")]
        )
    )
    # clean the contact_reason_id column
    calls_click_streams = calls_click_streams.withColumn(
        "contact_reason_id",
        F.when(
            F.col('contact_reason_id').contains('_'),
            F.col('contact_reason_id')
        ).otherwise(
            F.lit(None)
        )
    )

    # Renaming columns in order to be merged
    for columns in COLUMNS_TO_BE_RENAMED_CONTACT_REASON:
        contact_reason = contact_reason.withColumnRenamed(columns[0], columns[1])

    # Prepare the contact_reason dataset
    contact_reason = contact_reason.select(
        *COLUMNS_TO_BE_SELECTED_CONTACT_REASON
    )

    calls_click_streams = calls_click_streams.join(
        contact_reason,
        'contact_reason_id',
        'left'
    )
    calls_click_streams = standardize_country_code(calls_click_streams, iso_country_mapping)

    calls_click_streams = calls_click_streams.dropDuplicates().drop('veeva_product_id')
    calls_click_streams = get_gps_details(calls_click_streams, country)
    calls_click_streams = add_cdc_cols(calls_click_streams, 'calls_click_stream')
    return calls_click_streams


def stakeholder_plan(**kwargs):
    account_plan = kwargs['account_plan']
    account_tactic = kwargs['account_tactic']
    action_item = kwargs['action_item']
    plan_tactic = kwargs['plan_tactic']
    stakeholder = kwargs['stakeholder']
    record_type = kwargs['record_type']
    situation_analysis = kwargs['situation_analysis']
    user = kwargs['user']
    product_strategy = kwargs['product_strategy']
    account = kwargs['account']
    country = kwargs['country']
    COLUMNS_TO_BE_RENAMED_STAKEHOLDER_PLAN = [
        ('id', 'stakeholder_plan_id'),
        ('plan_name', 'stakeholder_plan_name'),
        ('account_id', 'stakeholder_id'),
        ('account_status', 'stakeholder_plan_status_type'),
        ('country', 'country_id'),
        ('coordinator', 'stakeholder_plan_coordinator'),
        ('description', 'stakeholder_plan_description'),
        ('create_date', 'stakeholder_plan_created_date'),
        ('product_catalog', 'product_id'),
        ('percent_complete', 'stakeholder_plan_percent_complete'),
        ('start_date', 'stakeholder_plan_start_date'),
        ('end_date', 'stakeholder_plan_end_date'),
        ('is_active', 'is_active_plan'),
        ("accountplan18id", "account_plan_id")
    ]

    COLUMNS_TO_BE_SELECTED_STAKEHOLDER_PLAN = ['stakeholder_plan_id',
                                               'stakeholder_plan_name',
                                               'stakeholder_id',
                                               'stakeholder_plan_status_type',
                                               'country_id',
                                               'created_by_id',
                                               'stakeholder_plan_coordinator',
                                               'stakeholder_plan_description',
                                               'row_is_current',
                                               'stakeholder_plan_created_date',
                                               'product_id',
                                               'stakeholder_plan_percent_complete',
                                               'record_type_id',
                                               'stakeholder_plan_start_date',
                                               'stakeholder_plan_end_date',
                                               'last_modified_date',
                                               'row_last_modified',
                                               'is_active_plan',
                                               'account_plan_id'
                                               ]

    COLUMNS_TO_BE_RENAMED_PLAN_TACTIC = [
        ('id', 'plan_tactic_id'),
        ('account_plan_id', 'stakeholder_plan_id'),
        ('abv_product', 'tactic_product'),
        ('abv_strategic_imperatives_description', 'strategic_imperatives_desc'),
        ('name', 'plan_tactic_name'),
        ('created_date', 'plan_tactic_date'),
        ('status', 'plan_tactic_status'),
        ('strategic_imperatives_template', 'strategic_imperatives_id')
    ]

    COLUMNS_TO_BE_SELECTED_PLAN_TACTIC = ['plan_tactic_id',
                                          'stakeholder_plan_id',
                                          'tactic_product',
                                          'owner_id',
                                          'strategic_imperatives_desc',
                                          'strategic_imperatives_description2',
                                          'plan_tactic_name',
                                          'plan_tactic_date',
                                          'priority',
                                          'plan_tactic_status',
                                          'total_account_tactics',
                                          'brand_ta',
                                          'strategic_imperatives_id',
                                          'row_is_current'
                                          ]

    COLUMNS_TO_BE_RENAMED_ACCOUNT_TACTIC = [
        ('id', 'stakeholder_tactic_id'),
        ('account_brand_plan_name', 'stakeholder_brand_plan_name'),
        ('account_brand_plan_product', 'stakeholder_brand_plan_product'),
        ('account_name', 'stakeholder_name'),
        ('functional_objective_description', 'functional_objective'),
        ('name', 'account_tactic_name'),
        ('share_with', 'stakeholder_tactic_share'),
        ('country', 'country_id'),
        ('owner_id', 'stakeholder_tactic_owner_id'),
        ('description', 'description_tactics'),
        ('status_code', 'sep_action_status_code_original')
    ]
    COLUMNS_TO_BE_SELECTED_ACCOUNT_TACTIC = [
        'stakeholder_tactic_id',
        'stakeholder_brand_plan_name',
        'stakeholder_brand_plan_product',
        'stakeholder_name',
        'functional_objective',
        'account_tactic_name',
        'stakeholder_tactic_share',
        'country_id',
        'stakeholder_tactic_owner_id',
        'row_is_current',
        'due_date',
        'plan_tactic_id',
        'outcome_tactic',
        'description_tactics',
        'sep_action_status_code_original',
        'product_strategy',
        'tactic_primary_responsible',
        'account_id',
        'account_plan_id']
    COLUMNS_TO_BE_RENAMED_ACTION_ITEM = [
        ('id', 'action_id'),
        ('account_id', 'action_stakeholder_id'),
        ('account_plan_id', 'action_stakeholder_plan_id'),
        ('account_tactic_id', 'action_stakeholder_plan_tactic_id'),
        ('owner_id', 'action_owner_id'),
        ('approval', 'action_approval_status'),
        ('status', 'action_status'),
        ('description', 'action_desc'),
        ('start_date', 'action_startdate')
    ]
    COLUMNS_TO_BE_SELECTED_ACTION_ITEM = [
        'action_id',
        'action_name',
        'assignee',
        'action_stakeholder_id',
        'action_stakeholder_plan_id',
        'action_stakeholder_plan_tactic_id',
        'action_owner_id',
        'action_approval_status',
        'action_status',
        'action_desc',
        'action_startdate',
        'row_is_current',
        'account_brand_plan']

    COLUMNS_TO_BE_RENAMED_RECORD_TYPE = [
        ('id', 'record_type_id'),
        ('name', 'plane_type'),
        ('row_is_current', 'row_is_current')
    ]
    COLUMNS_TO_BE_SELECTED_RECORD_TYPE = [col[1] for col in COLUMNS_TO_BE_RENAMED_RECORD_TYPE]

    COLUMNS_TO_BE_RENAMED_STAKEHOLDER = [
        ('stakeholder_id', 'stakeholder_id'),
        ('is_key_stakeholder', 'is_key_account'),
        ('registration_number', 'registration_number'),
        ('one_key_id', 'one_key_id'),
        ('stakeholder_name', 'mi_stakeholder_name')
    ]
    COLUMNS_TO_BE_SELECTED_STAKEHOLDER = [col[1] for col in COLUMNS_TO_BE_RENAMED_STAKEHOLDER]

    COLUMNS_TO_BE_SELECTED_SITUATION_ANALYSIS = [
        ("conclusion", "situation_conclusion"),
        ("external_oppurtinites", "situation_opportunities"),
        ("external_threats", "situation_threats"),
        ("internal_strengths", "situation_strengths"),
        ("internal_weaknesses", "situation_weaknesses"),
        ("engagement_plan", "stakeholder_plan_id"),
        ("last_modified_date", "situation_lastmodifieddate"),
        ("name", "situation_analysis_name"),
        ("objective", "situation_objective"),
        ("situation", "sep_situation")
    ]

    for columns in COLUMNS_TO_BE_SELECTED_SITUATION_ANALYSIS:
        situation_analysis = situation_analysis.withColumnRenamed(columns[0], columns[1])

    situation_analysis = situation_analysis.select(*[col[1] for col in COLUMNS_TO_BE_SELECTED_SITUATION_ANALYSIS])
    situation_analysis = situation_analysis.dropDuplicates()

    COLUMNS_TO_BE_SELECTED_USER = [
        ('employee_number', 'rep_employee_code'),
        ('name', 'rep_employee_name'),
        ('id', 'tactic_primary_responsible')
    ]

    for columns in COLUMNS_TO_BE_SELECTED_USER:
        user = user.withColumnRenamed(columns[0], columns[1])
    user = user.select(*[col[1] for col in COLUMNS_TO_BE_SELECTED_USER])
    user = user.dropDuplicates()
    user = user.withColumn('objectowner_rep_employee_code', F.col("rep_employee_code")).withColumn(
        'objectowner_rep_employee_name', F.col("rep_employee_name"))

    COLUMNS_TO_BE_SELECTED_ACCOUNT = [
        ('name', 'customer_name'),
        ('external_id', 'customer_code'),
        ('id', 'account_id')
    ]

    for columns in COLUMNS_TO_BE_SELECTED_ACCOUNT:
        account = account.withColumnRenamed(columns[0], columns[1])

    account = account.select(*[col[1] for col in COLUMNS_TO_BE_SELECTED_ACCOUNT])
    account = account.dropDuplicates()

    COLUMNS_TO_BE_SELECTED_PRODUCT_STRATEGY = [
        ('product_name', 'mi_marketed_product_name'),
        ('product_external_id', 'mi_marketed_product_code'),
        ('id', 'product_strategy'),
        ('name', 'csf_description'),
        ('description', 'sep_comments')
    ]

    for columns in COLUMNS_TO_BE_SELECTED_PRODUCT_STRATEGY:
        product_strategy = product_strategy.withColumnRenamed(columns[0], columns[1])

    product_strategy = product_strategy.select(*[col[1] for col in COLUMNS_TO_BE_SELECTED_PRODUCT_STRATEGY])
    product_strategy = product_strategy.dropDuplicates()
    # --------------------- account plan ---------------------- #

    # Renaming columns in order to be merged
    for columns in COLUMNS_TO_BE_RENAMED_STAKEHOLDER_PLAN:
        account_plan = account_plan.withColumnRenamed(columns[0], columns[1])

    # Select only a subset of columns
    account_plan = account_plan.select(
        *COLUMNS_TO_BE_SELECTED_STAKEHOLDER_PLAN)

    # Make sure there are no duplicates
    account_plan = account_plan.dropDuplicates()
    account_plan = account_plan.withColumn(
        "contact_reason_id",
        F.when(F.col('product_id').isNotNull(), F.concat_ws(
            '_',
            *[F.col("country_id"),
              F.col("product_id")]
        )).otherwise(F.col('product_id'))
    )
    account_plan = account_plan.withColumn("virtual_account_code", F.col('stakeholder_plan_id')).withColumn(
        "virtual_account_desc", F.col('stakeholder_plan_name'))

    # Filter only active records
    account_plan = account_plan.filter(F.col('row_is_current'))

    account_plan = add_in_stakeholder_id(account_plan, stakeholder)  # add function for veeva account id

    for columns in COLUMNS_TO_BE_RENAMED_STAKEHOLDER:
        stakeholder = stakeholder.withColumnRenamed(columns[0], columns[1])
    stakeholder = stakeholder.select(
        *COLUMNS_TO_BE_SELECTED_STAKEHOLDER)
    stakeholder = stakeholder.withColumn('stakeholder_code', F.col("one_key_id"))

    account_plan = account_plan.join(stakeholder, on=['stakeholder_id'], how='left').dropDuplicates()
    # --------------------- plan tactic---------------------- #

    # Renaming columns in order to be merged

    for columns in COLUMNS_TO_BE_RENAMED_PLAN_TACTIC:
        plan_tactic = plan_tactic.withColumnRenamed(columns[0], columns[1])

    # Select only a subset of columns
    plan_tactic = plan_tactic.select(
        *COLUMNS_TO_BE_SELECTED_PLAN_TACTIC)

    # Make sure there are no duplicates
    plan_tactic = plan_tactic.dropDuplicates()

    # Filter only active records
    plan_tactic = plan_tactic.filter(F.col('row_is_current')).drop('row_is_current')

    # --------------------- account tactic---------------------- #

    # Renaming columns in order to be merged
    for columns in COLUMNS_TO_BE_RENAMED_ACCOUNT_TACTIC:
        account_tactic = account_tactic.withColumnRenamed(columns[0], columns[1])

    # Select only a subset of columns
    account_tactic = account_tactic.select(
        *COLUMNS_TO_BE_SELECTED_ACCOUNT_TACTIC)

    # Make sure there are no duplicates
    account_tactic = account_tactic.dropDuplicates()

    # Filter only active records
    account_tactic = account_tactic.filter(F.col('row_is_current')).drop('row_is_current')
    account_tactic = account_tactic.withColumn('sep_description', F.col('account_tactic_name'))

    account_tactic = account_tactic.join(account, 'account_id', "left")
    account_tactic = account_tactic.join(user, 'tactic_primary_responsible', "left")
    account_tactic = account_tactic.join(product_strategy, 'product_strategy', "left")

    # --------------------- action_item---------------------- #

    # Renaming columns in order to be merged
    for columns in COLUMNS_TO_BE_RENAMED_ACTION_ITEM:
        action_item = action_item.withColumnRenamed(columns[0], columns[1])

    # Select only a subset of columns
    action_item = action_item.select(
        *COLUMNS_TO_BE_SELECTED_ACTION_ITEM)

    # Make sure there are no duplicates
    action_item = action_item.dropDuplicates()

    # Filter only active records
    action_item = action_item.filter(F.col('row_is_current')).drop('row_is_current')

    # --------------------- record_type---------------------- #

    # Renaming columns in order to be merged
    for columns in COLUMNS_TO_BE_RENAMED_RECORD_TYPE:
        record_type = record_type.withColumnRenamed(columns[0], columns[1])

    record_type = record_type.select(*COLUMNS_TO_BE_SELECTED_RECORD_TYPE)

    # Make sure there are no duplicates
    record_type = record_type.dropDuplicates()

    # Filter only active records
    record_type = record_type.filter(F.col('row_is_current')).drop('row_is_current')

    account_plan = account_plan.withColumn('stakeholder_plan_coordinator', F.when(
        F.col('stakeholder_plan_coordinator').isNull(), F.col('created_by_id')
    ).otherwise(F.col('stakeholder_plan_coordinator')))

    account_plan = account_plan.join(record_type, on=['record_type_id'], how='left')
    stakeholder_plan = account_plan.join(plan_tactic, on=['stakeholder_plan_id'], how='left').dropDuplicates()
    stakeholder_plan = stakeholder_plan.join(situation_analysis, on=['stakeholder_plan_id'], how='left')

    account_tactic_row = account_tactic.filter(F.col('plan_tactic_id').isNotNull()).drop('account_id')
    account_tactic_wec = account_tactic.filter(F.isnull(F.col('plan_tactic_id'))).withColumnRenamed('account_id',
                                                                                                    'stakeholder_id').drop(
        'plan_tactic_id', 'country_id')
    stakeholder_plan_wec = stakeholder_plan.filter(F.isnull(F.col('plan_tactic_id'))).withColumnRenamed(
        'stakeholder_id', 'plan_stakeholder_id')
    stakeholder_plan_row = stakeholder_plan.filter(F.col('plan_tactic_id').isNotNull())

    stakeholder_plan_wec = stakeholder_plan_wec.join(account_tactic_wec, on=['account_plan_id'], how='left')
    stakeholder_plan_wec = stakeholder_plan_wec.withColumn('stakeholder_id', F.coalesce(F.col('stakeholder_id'),
                                                                                        F.col('plan_stakeholder_id'))) \
        .dropDuplicates().drop('account_plan_id', 'plan_tactic_account_plan', 'mi_stakeholder_name',
                               'tactic_primary_responsible', 'account_id', 'plan_stakeholder_id')
    stakeholder_plan_row = stakeholder_plan_row.join(account_tactic_row, on=['plan_tactic_id', 'country_id'],
                                                     how='left') \
        .dropDuplicates().drop('account_plan_id', 'plan_tactic_account_plan', 'mi_stakeholder_name',
                               'tactic_primary_responsible', 'account_id')

    stakeholder_plan_row = stakeholder_plan_row.select(stakeholder_plan_wec.columns)
    stakeholder_plan = stakeholder_plan_wec.union(stakeholder_plan_row).distinct()

    stakeholder_plan = stakeholder_plan.withColumn('status_flag', F.col("row_is_current"))
    status_code = ['OPEN', 'Planned', 'Ongoing', 'On going', 'Started']
    stakeholder_plan = stakeholder_plan.withColumn('sep_action_status_code', F.when(
        (F.isnull(F.col('sep_action_status_code_original'))) | (
            F.col('sep_action_status_code_original').isin(status_code)), 'OPEN').when(
        F.col('sep_action_status_code_original') == 'Completed', 'CLSD').otherwise(
        F.col('sep_action_status_code_original')))
    stakeholder_plan = stakeholder_plan.withColumn('rep_employee_code', F.when((F.isnull(F.col('rep_employee_code'))),
                                                                               'No Employee').otherwise(
        F.col('rep_employee_code')))
    stakeholder_plan = stakeholder_plan.withColumn('rep_employee_name', F.when((F.isnull(F.col('rep_employee_name'))),
                                                                               'No Employee').otherwise(
        F.col('rep_employee_name')))
    # action_item = action_item.filter((F.col('account_plan_id').isNull()) or (F.col('account_tactic_id').isNull()))
    cond = [
        (stakeholder_plan.stakeholder_plan_id == action_item.account_brand_plan) |
        (stakeholder_plan.stakeholder_tactic_id == action_item.action_stakeholder_plan_tactic_id)]
    stakeholder_plan = stakeholder_plan.join(action_item, cond, 'left').drop("account_brand_plan").dropDuplicates()
    # stakeholder_plan1 = stakeholder_plan.withColumn("stakeholder_id", monotonically_increasing_id())
    stakeholder_plan = stakeholder_plan.withColumn("id", F.row_number().over(
        Window.orderBy(F.monotonically_increasing_id())))
    country
    return get_gps_details(stakeholder_plan, country)


def indication_ta(**kwargs):
    indication_ta = kwargs['abbvie_indications']
    return indication_ta


def patient_details(**kwargs):
    merged_apd_duodopa_patient_extract = kwargs['merged_apd_duodopa_patient_extract']
    immunology_patient_extract = kwargs['immunology_patient_extract']
    immunology_abbvie_care_extract = kwargs['immunology_abbvie_care_extract']
    hcv_patient_extract = kwargs['hcv_patient_extract']
    duodopa_measure = kwargs['duodopa_measure']
    immunology_measure = kwargs['immunology_measure']
    immunology_patient_extract_measure = kwargs['immunology_patient_extract_measure']
    hcv_patient_extract_measure = kwargs['hcv_patient_extract_measure']
    brand = kwargs['brand']
    qmidas_extract = kwargs['qmidas_extract']
    brand_molecule_mapping = kwargs['brand_molecule_mapping']
    country = kwargs['country']
    hcv_global = kwargs['hcv_global']
    hcv_japac = kwargs['hcv_japac']
    hcv_global_measure = kwargs['hcv_global_measure']
    oncology_global = kwargs['oncology_global']
    oncology_japac = kwargs['oncology_japac']
    area = kwargs['area']
    sky21_global = kwargs['sky21_global']
    duodopa_global = kwargs['duodopa_global']
    oncology_patients_affiliates = kwargs['oncology_patients_affiliates']
    oncology_patient_market_share = kwargs['oncology_patient_market_share']
    botox_patient_extract = kwargs['botox_patient_extract']
    retina_patient_extract = kwargs['retina_patient_extract']
    bu_by_src = kwargs['bu_by_src']
    # Duodopa from Mysource
    DUDOPA_COLUMN_RENAMES = [
        ('country_id', 'country_id'),
        ('measure', 'measure_from_source'),
        ('patient_count', 'patients_count'),
        ('time_period', 'month_year'),
        ('brand', 'brand'),
    ]
    # Duodopa external data
    DUDOPA_GLOBAL_COLUMN_RENAMES = [
        ('country_id', 'country_id'),
        ('unnamed_1', 'measure'),
        ("act", "act"),
        ("pln", "pln"),
        ("upd", "upd"),
        ("year", "year"),
        ("frequency", "frequency")
    ]
    # Rinvoq,Skyrizi,Humira from Mysource
    # required competitir information as well , hope available
    IMMUNOLOGY_COLUMN_RENAMES = [
        ('country_id', 'country_id'),
        ('product_orig_vs_bs_x', 'brand'),
        ('indication', 'indication'),
        ('indication_group', 'indication_group'),
        ('actual', 'actual'),
        ('plan', 'plan'),
        ('update', 'update'),
        ('time_period', 'month_year'),
        ('sub_indication', 'sub_indication'),
        ("product", "product")
    ]
    # should consider metic column for plan, update data
    IMMUNOLOGY_ABBVIE_CARE_COLUMN_RENAMES = [
        ('country_id', 'country_id'),
        ('product', 'brand'),
        ('indication', 'indication'),
        ('time_period', 'month_year'),
        ('value_data', 'patients_count'),
        ('metric', 'measure_from_source')
    ]

    HCV_COLUMN_RENAMES = [
        ('country_id', 'country_id'),
        ('regimen', 'brand'),
        ('regimen_group', 'brand_group'),
        ('patients_actual', 'patients_actual'),
        ('market_packs', 'market_packs'),
        ('time_period', 'month_year')
    ]

    SKY21_COLUMN_RENAMES = [
        ('country_id', 'country_id'),
        ('metric', 'measure_from_source'),
        ('metric_short_name', 'measure'),
        ('values', 'patients_count'),
        ('time_period', 'month_year'),
        ('brand_name', 'brand'),
        ('indication', 'indication'),
        ('line_of_theraphy', 'line_of_theraphy'),
        ('scenario', 'scenario'),
        ('molecule_long_name', 'molecule_long_name'),
        ('cluster', 'cluster'),
        ('area_name', 'area_name')
    ]

    HCV_GLOBAL_COLUMN_RENAMES = [
        ("scoped_or_non_scoped", "is_scoped"),
        ("drug", "brand"),
        ("country_id", "country_id"),
        ("act", "act"),
        ("pln", "pln"),
        ("upd", "upd"),
        ("year", "year"),
        ("frequency", "frequency")
    ]

    HCV_JAPAC_COLUMN_RENAMES = [
        ("drug", "brand"),
        ("country_id", "country_id"),
        ("pln", "pln"),
        ("year", "year"),
        ("frequency", "frequency")
    ]

    ONCOLOGY_GLOBAL_COLUMN_RENAMES = [
        ('country_id', 'country_id'),
        ('indication', 'indication'),
        ('scenario', 'scenario'),
        ('values', 'patients_count'),
        ('measure', 'measure'),
        ('time_period', 'month_year'),
        ('competitor', 'competitor'),
        ('financing', 'financing'),
        ('area_name', 'area_name')
    ]

    ONCOLOGY_JAPAC_COLUMN_RENAMES = [
        ('country_id', 'country_id'),
        ('indication', 'indication'),
        ('scenario', 'scenario'),
        ('values', 'patients_count'),
        ('measure', 'measure'),
        ('time_period', 'month_year'),
        ('brand', 'brand'),
        ('line_of_therapy', 'line_of_theraphy'),
        ('area_name', 'area_name')
    ]

    ONCOLOGY_PATIENTS_AFFILIATES_COLUMN_RENAMES = [
        ('country_id', 'country_id'),
        ('line_of_therapy', 'line_of_theraphy'),
        ('time_period', 'month_year'),
    ]

    ONCOLOGY_PATIENTS_MARKET_SHARE_COLUMN_RENAMES = [
        ('country_id', 'country_id'),
        ('line_of_therapy', 'line_of_theraphy'),
        ('time_period', 'month_year'),
    ]

    BOTOX_COLUMN_RENAMES = [
        ('time_period', 'month_year'),
        ('patient_count', 'patients_count')
    ]

    RETINA_COLUMN_RENAMES = [
        ('time_period', 'month_year'),
        ('patient_count', 'patients_count')
    ]

    SELECT_LIST = ['country_id', 'brand', 'indication_group', 'indication', 'month_year', 'sub_indication', 'product']
    SELECT_LIST_ONCO_PATIENT = ['country_id', 'source', 'line_of_theraphy', 'indication', 'patients_count', 'scenario',
                                'measure', 'month_year', 'business_unit']
    SELECT_LIST_ONCO_MARKET = ['country_id', 'brand', 'source', 'line_of_theraphy', 'indication', 'patients_count',
                               'scenario', 'measure', 'month_year', 'business_unit']
    SELECT_LIST_BOTOX = ['country_id', 'brand', 'indication', 'patients_count', 'month_year', 'measure']
    SELECT_LIST_RETINA = ['country_id', 'brand', 'indication', 'patients_count', 'month_year', 'measure']

    DUDOPA_COLUMN_TO_BE_SELECTED = [col[1] for col in DUDOPA_COLUMN_RENAMES]
    DUDOPA_GLOBAL_COLUMN_TO_BE_SELECTED = [col[1] for col in DUDOPA_GLOBAL_COLUMN_RENAMES]
    IMMUNOLOGY_COLUMN_SELECTED = [col[1] for col in IMMUNOLOGY_COLUMN_RENAMES]
    IMMUNOLOGY_ABBVIE_CARE_COLUMN_SELECTED = [col[1] for col in IMMUNOLOGY_ABBVIE_CARE_COLUMN_RENAMES]
    HCV_COLUMN_TO_BE_SELECTED = [col[1] for col in HCV_COLUMN_RENAMES]
    SKYRIZI_COLUMN_TO_BE_SELECTED = [col[1] for col in SKY21_COLUMN_RENAMES]
    HCV_GLOBAL_COLUMN_TO_BE_SELECTED = [col[1] for col in HCV_GLOBAL_COLUMN_RENAMES]
    HCV_JAPAC_COLUMN_TO_BE_SELECTED = [col[1] for col in HCV_JAPAC_COLUMN_RENAMES]
    ONCOLOGY_GLOBAL_COLUMN_TO_BE_SELECTED = [col[1] for col in ONCOLOGY_GLOBAL_COLUMN_RENAMES]
    ONCOLOGY_JAPAC_COLUMN_TO_BE_SELECTED = [col[1] for col in ONCOLOGY_JAPAC_COLUMN_RENAMES]
    ONCOLOGY_PATIENTS_AFFILIATES_SELECTED = [col[1] for col in ONCOLOGY_PATIENTS_AFFILIATES_COLUMN_RENAMES]
    ONCOLOGY_PATIENTS_MARKET_SHARE_SELECTED = [col[1] for col in ONCOLOGY_PATIENTS_MARKET_SHARE_COLUMN_RENAMES]
    BOTOX_COLUMN_TO_BE_SELECTED = [col[1] for col in BOTOX_COLUMN_RENAMES]
    RETINA_COLUMN_TO_BE_SELECTED = [col[1] for col in RETINA_COLUMN_RENAMES]
    brand_ta = brand.select('brand', 'therapeutic_area').distinct()
    brand_molecule_mapping = brand_molecule_mapping.withColumn('indication', F.coalesce(F.col('indication'), F.lit('')))
    # ----------------------- DUODOPA from Mysource---------------------#
    for columns in DUDOPA_COLUMN_RENAMES:
        merged_apd_duodopa_patient_extract = merged_apd_duodopa_patient_extract.withColumnRenamed(columns[0],
                                                                                                  columns[1])

    merged_apd_duodopa_patient_extract = merged_apd_duodopa_patient_extract.select(
        DUDOPA_COLUMN_TO_BE_SELECTED
    ).dropDuplicates()
    merged_apd_duodopa_patient_extract = merged_apd_duodopa_patient_extract.join(
        duodopa_measure, 'measure_from_source'
    )
    merged_apd_duodopa_patient_extract = merged_apd_duodopa_patient_extract.withColumn('source',
                                                                                       F.lit('specialty_patients'))

    # ----------------------- DUODOPA from external source---------------------#
    for columns in DUDOPA_GLOBAL_COLUMN_RENAMES:
        duodopa_global = duodopa_global.withColumnRenamed(columns[0], columns[1])

    duodopa_global = duodopa_global.filter(F.col('country_id').isNotNull()).select(
        DUDOPA_GLOBAL_COLUMN_TO_BE_SELECTED
    ).withColumn('brand', F.lit('duodopa')). \
        withColumn('month_year', F.concat(F.col('frequency'), F.col('year'))).drop('year', 'frequency'). \
        dropDuplicates()
    duodopa_global = duodopa_global.filter(F.col('frequency') != 'FY')
    duodopa_global = to_explode(
        duodopa_global, ['country_id', 'month_year', 'measure', 'brand']). \
        withColumn('scenario', F.when(
        F.col('measure_from_source') == 'act', F.lit('Actual')
    ).when(F.col('measure_from_source') == 'pln', F.lit('Plan')).otherwise(F.lit('Update'))). \
        withColumn('measure_from_source', F.col('measure')). \
        withColumn('source', F.lit('duodopa_global')). \
        withColumn('patients_count', F.coalesce(F.col('patients_count'), F.lit(0)).cast('double')).dropDuplicates()
    # ----------------------- IMMUNOLOGY (Rinvoq,Skyrizi,Humira ) from Mysource---------------------#
    # Competitor information is required for Rinvoq, Skyrizi, Humira, Maviret
    for columns in IMMUNOLOGY_COLUMN_RENAMES:
        immunology_patient_extract = immunology_patient_extract.withColumnRenamed(columns[0], columns[1])

    immunology_patient_extract = immunology_patient_extract.select(
        IMMUNOLOGY_COLUMN_SELECTED
    )
    immunology_patient_extract = to_explode(
        immunology_patient_extract, SELECT_LIST)

    immunology_patient_extract = immunology_patient_extract.withColumn('source', F.lit('immunology_patients')). \
        join(immunology_patient_extract_measure, 'measure_from_source'). \
        withColumn('patients_count', F.coalesce(F.col('patients_count'), F.lit(0)).cast('double'))

    for columns in IMMUNOLOGY_ABBVIE_CARE_COLUMN_RENAMES:
        immunology_abbvie_care_extract = immunology_abbvie_care_extract.withColumnRenamed(columns[0], columns[1])

    immunology_abbvie_care_extract = immunology_abbvie_care_extract.select(
        IMMUNOLOGY_ABBVIE_CARE_COLUMN_SELECTED
    ). \
        dropDuplicates()
    # need to consider metric column ?
    immunology_abbvie_care_extract = immunology_abbvie_care_extract.join(immunology_measure, 'measure_from_source'). \
        filter(F.col('measure') != 'PERCENTAGE')

    immunology_abbvie_care_extract = immunology_abbvie_care_extract.withColumn('source',
                                                                               F.lit('immunology_abbvie_care'))
    immunology_patient_extract = safe_union(immunology_patient_extract, immunology_abbvie_care_extract)

    # ----------------------- HCV(MAVIRET) from Mysource---------------------#
    for columns in HCV_COLUMN_RENAMES:
        hcv_patient_extract = hcv_patient_extract.withColumnRenamed(columns[0], columns[1])

    hcv_patient_extract = hcv_patient_extract.select(
        HCV_COLUMN_TO_BE_SELECTED
    )
    hcv_patient_extract = to_explode(hcv_patient_extract, ['country_id', 'month_year', 'brand', 'brand_group'])
    hcv_patient_extract = hcv_patient_extract.join(hcv_patient_extract_measure, 'measure_from_source')
    hcv_patient_extract = hcv_patient_extract.withColumn('source', F.lit('hcv_patients')). \
        withColumn('patients_count', F.coalesce(F.col('patients_count'), F.lit(0)).cast('double'))
    # business is only looking for In Market Treated Patients for the brand Maviret and To Market Patients
    # MAVYRET/MAVIRET to be transformed as MAVIRET to standardize , This might be corrected in src
    # hcv_global = hcv_global.filter(F.col('information').isin(['To Market Patients', 'In Market Treated Patients']))
    hcv_global = hcv_global.withColumn('drug', F.when(F.col('drug').isin("Maviret", "MAVYRET/MAVIRET"),
                                                      F.lit('MAVIRET')).otherwise(F.col('drug')))
    hcv_global = hcv_global.filter(F.col('drug').isin(['MAVIRET', 'TOTAL']))

    # ----------------------- HCV commercial model---------------------#
    for columns in HCV_GLOBAL_COLUMN_RENAMES:
        hcv_global = hcv_global.withColumnRenamed(columns[0], columns[1])

    hcv_global = hcv_global.filter(F.col('frequency').isin(['Q1', 'Q2', 'Q3', 'Q4', 'FY']) == False).select(
        HCV_GLOBAL_COLUMN_TO_BE_SELECTED
    ).withColumn('month_year', F.concat(F.col('frequency'), F.col('year'))).drop('year', 'frequency'). \
        withColumn('is_scoped', F.when(F.col('is_scoped') == 'Scoped', True).otherwise(False)). \
        dropDuplicates()
    hcv_global = to_explode(hcv_global, ['brand', 'country_id', 'month_year', 'is_scoped'])
    hcv_global = hcv_global.join(
        hcv_global_measure, 'measure_from_source'
    )
    hcv_global = hcv_global.withColumn('source', F.lit('hcv_global')). \
        withColumn('patients_count', F.coalesce(F.col('patients_count'), F.lit(0)).cast('double')). \
        dropDuplicates()

    # hcv japac
    hcv_japac = hcv_japac.filter(F.col('information').isin(['Total Market Patients', 'In Market Patients'])). \
        withColumn('drug',
                   F.when(F.col('drug').contains('MAVYRET/MAVIRET'), F.lit('MAVIRET')).otherwise(F.col('drug'))). \
        filter(F.col('drug').isin(['MAVIRET', 'TOTAL']))

    # ----------------------- HCV commercial model---------------------#
    for columns in HCV_JAPAC_COLUMN_RENAMES:
        hcv_japac = hcv_japac.withColumnRenamed(columns[0], columns[1])

    hcv_japac = hcv_japac.filter(F.col('frequency').isin(['FY']) == False).select(
        HCV_JAPAC_COLUMN_TO_BE_SELECTED
    ).withColumn('month_year', F.concat(F.col('frequency'), F.col('year'))).drop('year', 'frequency').dropDuplicates()
    hcv_japac = to_explode(hcv_japac, ['brand', 'country_id', 'month_year'])
    hcv_japac = hcv_japac.join(
        hcv_global_measure, 'measure_from_source'
    )
    hcv_japac = hcv_japac.withColumn('source', F.lit('hcv_japac')). \
        withColumn('patients_count', F.coalesce(F.col('patients_count'), F.lit(0)).cast('double')). \
        dropDuplicates()

    # sky21 global
    # ----------------------- HCV commercial model---------------------#
    for columns in SKY21_COLUMN_RENAMES:
        sky21_global = sky21_global.withColumnRenamed(columns[0], columns[1])

    sky21_global = sky21_global.select(
        SKYRIZI_COLUMN_TO_BE_SELECTED
    ).dropDuplicates()
    sky21_global = sky21_global.withColumn('source', F.lit('sky21_global')). \
        withColumn('patients_count', F.coalesce(F.col('patients_count'), F.lit(0)).cast('double')). \
        withColumn('scenario', F.when(F.col('scenario') == 'ACT', F.lit('Actual')).otherwise(F.lit('Plan'))). \
        dropDuplicates()

    # eemea_oncology
    # --------------EEMEA ONCOLOGY FOR VENCLEXTA/VENCLYXTO-------------------- #
    for columns in ONCOLOGY_GLOBAL_COLUMN_RENAMES:
        oncology_global = oncology_global.withColumnRenamed(columns[0], columns[1])

    oncology_global = oncology_global.select(
        ONCOLOGY_GLOBAL_COLUMN_TO_BE_SELECTED
    ).dropDuplicates()

    oncology_eemea = oncology_global.withColumn('source', F.lit('oncology_eemea')). \
        withColumn('patients_count', F.coalesce(F.col('patients_count'), F.lit(0)).cast('double')). \
        withColumn('scenario', F.when(F.col('scenario') == 'ACT', F.lit('Actual')).otherwise(F.lit('Plan'))). \
        withColumn('line_of_theraphy', F.lit('TOTAL')). \
        withColumn("brand", F.when(F.col("competitor") == "Oral CLL", F.lit('TOTAL'))
                   .when((F.col("competitor") == "Ven") & (F.col("financing") == "Commercial"), F.lit(
        'VENCLEXTA/VENCLYXTO')).otherwise(F.lit('VENCLYXTO'))). \
        withColumn("measure", F.when(F.col("competitor") == "Oral CLL", F.lit('Market size (Oral treatments)'))
                   .when((F.col("competitor") == "Ven") & (F.col("financing") == "Commercial") & (
            F.col('measure') == "End of Month Pts"), F.lit(
        'End of month patients'))
                   .when((F.col("competitor") == "Ven") & (F.col("financing") == "Commercial") & (
            F.col('measure') == "Dropouts"), F.lit(
        'Dropout patients'))
                   .when((F.col("competitor") == "Ven") & (F.col("financing") == "Commercial") & (
            F.col('measure') == "Starting Pts"), F.lit(
        'Starting patients')).otherwise(F.col('measure'))). \
        withColumn("product", F.col('brand')). \
        dropDuplicates()

    # japac_oncology
    # --------------JAPAC ONCOLOGY FOR VENCLEXTA/VENCLYXTO-------------------- #
    for columns in ONCOLOGY_JAPAC_COLUMN_RENAMES:
        oncology_japac = oncology_japac.withColumnRenamed(columns[0], columns[1])

    oncology_japac = oncology_japac.select(
        ONCOLOGY_JAPAC_COLUMN_TO_BE_SELECTED
    ).dropDuplicates()

    oncology_japac = oncology_japac.withColumn('source', F.lit('oncology_japac')). \
        withColumn('patients_count', F.coalesce(F.col('patients_count'), F.lit(0)).cast('double')). \
        withColumn('brand', F.lit('VENCLEXTA/VENCLYXTO')). \
        withColumn("product", F.col('brand')). \
        dropDuplicates()

    # oncology_patients_affiliates

    for columns in ONCOLOGY_PATIENTS_AFFILIATES_COLUMN_RENAMES:
        oncology_patients_affiliates = oncology_patients_affiliates.withColumnRenamed(columns[0], columns[1])

    oncology_patients_affiliates = oncology_patients_affiliates.select(
        SELECT_LIST_ONCO_PATIENT
    ).dropDuplicates()

    oncology_patients_affiliates = oncology_patients_affiliates.withColumn('patients_count',
                                                                           F.coalesce(F.col('patients_count'),
                                                                                      F.lit(0)).cast('double')) \
        .drop('business_unit')

    # oncology_patient_market_share

    for columns in ONCOLOGY_PATIENTS_MARKET_SHARE_COLUMN_RENAMES:
        oncology_patient_market_share = oncology_patient_market_share.withColumnRenamed(columns[0], columns[1])

    oncology_patient_market_share = oncology_patient_market_share.select(
        SELECT_LIST_ONCO_MARKET
    ).dropDuplicates()

    oncology_patient_market_share = oncology_patient_market_share.withColumn('patients_count',
                                                                             F.coalesce(F.col('patients_count'),
                                                                                        F.lit(0)).cast('double')) \
        .drop('business_unit')

    # ----------------------- BOTOX from Mysource---------------------#
    # botox_patient_extract
    for columns in BOTOX_COLUMN_RENAMES:
        botox_patient_extract = botox_patient_extract.withColumnRenamed(columns[0], columns[1])

    botox_patient_extract = botox_patient_extract.select(
        SELECT_LIST_BOTOX
    ).dropDuplicates()

    botox_patient_extract = botox_patient_extract.withColumn('source', F.lit('botox_patients')). \
        withColumn('patients_count', F.coalesce(F.col('patients_count'), F.lit(0)).cast('double')). \
        withColumn('scenario', F.lit('Actual')). \
        dropDuplicates()

    # ----------------------- RETINA from Mysource---------------------#
    # retina_patient_extract
    for columns in RETINA_COLUMN_RENAMES:
        retina_patient_extract = retina_patient_extract.withColumnRenamed(columns[0], columns[1])

    retina_patient_extract = retina_patient_extract.select(
        SELECT_LIST_RETINA
    ).dropDuplicates()

    retina_patient_extract = retina_patient_extract.withColumn('source', F.lit('retina_patients')). \
        withColumn('patients_count', F.coalesce(F.col('patients_count'), F.lit(0)).cast('double')). \
        withColumn('scenario', F.lit('Actual')). \
        dropDuplicates()

    # Union all
    patient_data = safe_union(merged_apd_duodopa_patient_extract, immunology_patient_extract)
    patient_data = safe_union(patient_data, hcv_patient_extract)
    patient_data = safe_union(patient_data, hcv_global)
    patient_data = safe_union(patient_data, hcv_japac)
    patient_data = safe_union(patient_data, duodopa_global)
    patient_data = safe_union(patient_data, oncology_patients_affiliates)
    patient_data = safe_union(patient_data, oncology_patient_market_share)
    patient_data = safe_union(patient_data, botox_patient_extract)
    patient_data = safe_union(patient_data, retina_patient_extract)
    patient_data = patient_data. \
        filter(F.col('country_id').isNotNull())

    patient_data = safe_union(patient_data, sky21_global)
    patient_data = safe_union(patient_data, oncology_eemea)
    patient_data = safe_union(patient_data, oncology_japac)
    patient_data = patient_data. \
        withColumn('patients_count', F.coalesce(F.col('patients_count'), F.lit(0)).cast('double')). \
        withColumn('month', F.substring(F.col('month_year'), 0, 2)). \
        withColumn('year', F.substring(F.col('month_year'), 3, 5)). \
        withColumn('date', F.to_date(F.concat_ws('-', F.col('year'), F.col('month'), F.lit('01')))). \
        withColumn('brand', F.upper(F.col('brand'))). \
        withColumn('indication', F.coalesce(F.col('indication'), F.lit(''))). \
        drop('month', 'year', 'month_year')

    # Enrich the brand dataset
    qmidas_extract = qmidas_extract.select(
        F.upper(F.col('product_intl')).alias('brand'), 'molecule', F.col('atc_4').alias('mechanism_of_action')
    ).withColumn('brand', F.when(
        F.col('brand') == 'VENCLEXTA', F.lit('VENCLEXTA/VENCLYXTO')).otherwise(F.col('brand'))
                 ).dropDuplicates()
    brand = brand.select('brand'). \
        withColumn('brand',
                   F.when(F.col('brand') == 'VENCLEXTA', F.lit('VENCLEXTA/VENCLYXTO')).otherwise(F.col('brand'))). \
        dropDuplicates().join(qmidas_extract, 'brand', 'left'). \
        withColumn('molecule', F.coalesce(F.col('molecule'), F.lit('to_be_mapped'))). \
        withColumn('mechanism_of_action', F.coalesce(F.col('mechanism_of_action'), F.lit('')))

    brand = brand.groupBy('brand'). \
        agg(
        F.concat_ws('_', F.collect_set('molecule')).alias('molecule'),
        F.concat_ws('_', F.collect_set('mechanism_of_action')).alias('mechanism_of_action'),
    ).dropDuplicates()

    patient_data = patient_data.join(brand, 'brand', 'left'). \
        join(brand_molecule_mapping, ['source', 'brand', 'indication'], 'left'). \
        withColumn('indication', F.coalesce(F.col('mapped_indication'), F.col('indication'))). \
        withColumn('mechanism_of_action', F.when(
        F.col('source').isin(['hcv_patients', 'hcv_global', 'hcv_japac']), F.lit('not applicable')
    ).otherwise(
        F.coalesce(F.col('mapped_moa'), F.col('mechanism_of_action'))
    )).drop('mapped_indication')
    patient_data = patient_data. \
        withColumn('molecule', F.when(
        F.col('source').isin(['hcv_patients', 'hcv_global', 'hcv_japac']), F.lit('not applicable')
    ).when(
        F.col('source').isin(['sky21_global']), F.col('molecule_long_name')
    ).otherwise(F.coalesce(F.col('mapped_molecule'), F.col('molecule')))). \
        withColumn('indication', F.when(
        F.col('source').isin(['hcv_patients', 'hcv_global', 'hcv_japac']), F.lit('Hepatitis C Virus (HCV)')
    ).when(
        F.col('source').isin(['specialty_patients', 'duodopa_global']), F.lit('Parkinsons Disease (PD)')
    ).otherwise(F.col('indication'))). \
        drop('mapped_molecule', 'mapped_moa', 'molecule_long_name')
    patient_data = patient_data.withColumn("molecule", F.when(
        (F.col("source") == "oncology_eemea") & (F.col("competitor") == "Oral CLL"), F.lit('TOTAL'))
                                           .otherwise(F.col('molecule'))).drop('competitor', 'financing')
    patient_data = patient_data.withColumn('mechanism_of_action',
                                           F.when(F.col('source').isin(['oncology_patient_starts']),
                                                  F.lit(None)).otherwise(F.col('mechanism_of_action'))) \
        .withColumn('molecule',
                    F.when(F.col('source').isin(['oncology_patient_starts']), F.lit(None)).otherwise(F.col('molecule')))
    patient_data = patient_data.join(country.select('country_id', 'area', 'region', 'affiliate_group'), 'country_id',
                                     'left')
    patient_data = patient_data.withColumn('area', F.when(
        (F.col('source') == 'oncology_eemea'), F.lit('EEME&A')
    ).otherwise(F.col('area')))
    # for sky21 data to be available at region level where country id will be null. So removing null country_id filter
    # and adding cluster column values to region column
    patient_data = patient_data.withColumn('region', F.coalesce('cluster', 'region'))
    patient_data = patient_data.withColumn('region', F.when(F.col('region') == 'RS', F.lit('South')).
                                           when(F.col('region') == 'RN', F.lit('North')).
                                           when(F.col('region') == 'LBUs', F.lit('LBU')).otherwise(
        F.col('region'))).drop('cluster')

    # obtaining area column for records where country_id is null
    patient_data = patient_data.withColumn('area_name', F.when((F.col('source') == 'sky21_global') & (F.col(
        'area_name') == 'WEC'), F.lit('WE&C')).otherwise(F.col('area_name')))
    patient_data = patient_data.withColumn('area', F.when(
        (F.col('source') == 'oncology_eemea') | (F.col('source') == 'sky21_global') | (F.col(
            'source') == 'oncology_japac'),
        F.col('area_name')).otherwise(F.col('area'))).drop('area_name')
    patient_data = patient_data.join(area.select('area', 'area_id'), 'area', 'left')
    patient_data = patient_data.join(brand_ta, 'brand', 'left')
    patient_data = patient_data.join(bu_by_src, 'source', 'left')
    patient_data = patient_data.withColumn('patient_details_id', F.sha2(
        F.concat_ws("_", *patient_data.columns), 256))
    patient_data = patient_data.withColumn("business_unit", F.when(
        (F.col("source") == "specialty_patients") & (F.col("business_unit").isNull()), F.lit("NEUROSCIENCE")).otherwise(
        F.col("business_unit")))
    patient_data = patient_data.withColumn("business_unit", F.when(
        (F.col("source") == "oncology_patient_starts") & (F.col("business_unit").isNull()),
        F.lit("ONCOLOGY")).otherwise(F.col("business_unit")))
    return get_gps_details(patient_data.dropDuplicates(), country)


# convert the columns to rows
def to_explode(df, by):
    # Filter dtypes and split into column names and type description
    cols, dtypes = zip(*((c, t) for (c, t) in df.dtypes if c not in by))
    patient_extract = F.explode(F.array([
        F.struct(F.lit(c).alias("measure_from_source"), F.col(c).alias("patients_count")) for c in cols
    ])).alias("patient_extract")

    return df.select(by + [
        patient_extract]
                     ).select(
        by +
        ["patient_extract.measure_from_source", "patient_extract.patients_count"]
    )


def molecule(**kwargs):
    patient_data = kwargs['patient_details']
    dynamic_market_share = kwargs['dynamic_market_share']
    product = kwargs['product']
    molecule = patient_data.filter(F.col('molecule').isNotNull()).select('molecule')
    dynamic_market_share = dynamic_market_share \
        .filter((F.col('molecule').isNotNull()) | (F.length('molecule') > 0)) \
        .select('molecule')
    product = product \
        .filter((F.col('molecule').isNotNull()) | (F.length('molecule') > 0)) \
        .select('molecule')
    molecule = molecule.unionByName(dynamic_market_share)
    molecule = molecule.unionByName(product).dropDuplicates()
    return molecule


def survey(**kwargs):
    survey = kwargs['survey']
    survey_question = kwargs['survey_question']
    survey_responses = kwargs['survey_responses']
    survey_targets = kwargs['survey_targets']
    on24_survey = kwargs['on24_survey']
    on24_polls = kwargs['on24_polls']
    attendee_questions = kwargs['attendee_questions']
    webinar = kwargs['webinar']
    stakeholder = kwargs['stakeholder']
    record_type = kwargs['record_type']
    country = kwargs['country']
    one_emp_to_many_terr = kwargs['one_emp_to_many_terr']
    end_year = 9999

    COLUMNS_TO_BE_RENAMED_SURVEY = [
        ("country", "country_id"),
        ("id", "survey_code"),
        ("name", "survey_name"),
        ("product_id", "contact_reason_id"),
        ("record_type_id", "record_type_id"),
        ("start_date", "start_date"),
        ("create_date", "create_date"),
        ("end_date", "end_date"),
        ("expired", "is_survey_expired"),
        ("status", "status"),
        ("territories", "territories"),
        ("territory_id", "territory_id"),
        ("assignment_type", "assignment_type"),
        ("channels", "channels"),
    ]
    COLUMNS_TO_BE_SELECTED_SURVEY = [col[1] for col in COLUMNS_TO_BE_RENAMED_SURVEY]

    COLUMNS_TO_BE_RENAMED_SURVEY_QUEST = [
        ("id", "survey_question_code"),
        ("name", "survey_question_name"),
        ("answer_choice", "answer_choices"),
        ("questionorder", "order"),
        ("is_required", "is_required"),
        ("text", "question_text"),
        ("record_type_id", "record_type_id"),
        ("internal_id", "internal_id")
    ]
    COLUMNS_TO_BE_SELECTED_SURVEY_QUEST = [col[1] for col in COLUMNS_TO_BE_RENAMED_SURVEY_QUEST]

    COLUMNS_TO_BE_RENAMED_SURVEY_RESPONSE = [
        ("id", "survey_response_code"),
        ("survey_question", "survey_question_code"),
        ("name", "survey_response_name"),
        ("survey_target_id", "survey_target_code"),
        ("response", "answer_choice_response"),
        ("text", "free_text_response"),
        ("number", "number_response"),
        ('record_type_id', 'record_type_id'),
        ('create_date', 'response_create_date'),
        ('created_by_id', 'employee_id'),
        ('datetime', 'response_date_time'),
        ('date', 'response_date'),
        ('decimal', 'response_decimal'),
        # 99.7% null , required for Back to the Workplace - Reporting Period 21: Oct18-Oct24
        ('score', 'score'),
        ("row_is_current", "row_is_current"),
        ("row_last_modified", "row_last_modified"),
        ("country", "survey_response_country_id")
    ]
    COLUMNS_TO_BE_SELECTED_SURVEY_RESPONSE = [col[1] for col in COLUMNS_TO_BE_RENAMED_SURVEY_RESPONSE]

    COLUMNS_TO_BE_RENAMED_SURVEY_TARGETS = [
        ("account_id", "stakeholder_id"),
        ("id", "survey_target_code"),
        ("name", "survey_target_name"),
        ("survey_id", "survey_code"),
        ("record_type_id", "record_type_id"),
        ('not_completed', 'is_not_completed'),
        ('owner_id', 'owner_employee_id'),
        ('report_status', 'report_status'),
        ('review_date', 'review_date'),
        ('coach', 'manager_id'),
        ('suggestions_id', 'suggestions_id'),
        ('status', 'survey_target_status')
    ]
    COLUMNS_TO_BE_SELECTED_SURVEY_TARGETS = [col[1] for col in COLUMNS_TO_BE_RENAMED_SURVEY_TARGETS]

    COLUMNS_TO_BE_RENAMED_ON24_SURVEY = [
        ("country_id", "country_id"),
        ('survey_id', 'survey_code'),
        ("survey_question_id", "survey_question_code"),
        ("survey_question_text", "question_text"),
        ('survey_answers_text', 'survey_response_name'),
        ('survey_detail_answer_text', 'free_text_response'),
        ("survey_detail_answer_code", "answer_choice_response"),
        ('survey_submitted_timestamp', 'response_date'),
        ('event_id', 'event_id'),
        ('event_user_id', 'event_user_id'),
        ('survey_response_code', 'survey_response_code'),
        ("row_is_current", "row_is_current"),
        ("row_last_modified", "row_last_modified")
    ]
    COLUMNS_TO_BE_SELECTED_ON24_SURVEY = [col[1] for col in COLUMNS_TO_BE_RENAMED_ON24_SURVEY]

    ON24_SURVEY_GRP_BY_COLS = [
        'country_id',
        'survey_code',
        'survey_question_code',
        'answer_choices',
        'survey_response_code',
        'survey_id',
        'event_id',
        'event_user_id',
        'question_text',
        'response_date',
    ]

    COLUMNS_TO_BE_RENAMED_ON24_POLLS = [
        ("country_id", "country_id"),
        ('poll_id', 'survey_code'),
        ("poll_question_id", "survey_question_code"),
        ("poll_question", "question_text"),
        ('poll_answers', 'survey_response_name'),
        ('details_answer', 'free_text_response'),
        ("details_answer_code", "answer_choice_response"),
        ('poll_submitted_timestamp', 'response_date'),
        ('event_id', 'event_id'),
        ('event_user_id', 'event_user_id'),
        ('survey_response_code', 'survey_response_code'),
        ("row_is_current", "row_is_current"),
        ("row_last_modified", "row_last_modified")
    ]
    COLUMNS_TO_BE_SELECTED_ON24_POLLS = [col[1] for col in COLUMNS_TO_BE_RENAMED_ON24_POLLS]

    COLUMNS_TO_BE_RENAMED_ONE_EMP_TO_MANY_TERR = [
        ("employee_id", "employee_id"),
        ("start_date", "response_start_date"),
        ("end_date", "response_end_date"),
        ("veeva_territory_id", "response_collected_veeva_territory_id")
    ]
    COLUMNS_TO_BE_SELECTED_ONE_EMP_TO_MANY_TERR = [col[1] for col in COLUMNS_TO_BE_RENAMED_ONE_EMP_TO_MANY_TERR]

    # on24 attendee questions
    COLUMNS_TO_BE_RENAMED_ON24_ATTENDEE_QUESTIONS = [
        ("country_id", "country_id"),
        ("question_id", "survey_question_code"),
        ("content", "question_text"),
        ('answer_text', 'free_text_response'),
        ('answer_timestamp', 'response_date_time'),
        ('event_id', 'event_id'),
        ('event_user_id', 'event_user_id'),
        ('answer_privacy', 'answer_privacy'),
        ('answer_presenter_id', 'answer_presenter_id'),
        ('answer_presenter_name', 'answer_presenter_name'),
        ('create_timestamp', 'create_date')
    ]
    COLUMNS_TO_BE_SELECTED_ON24_ATTENDEE_QUESTIONS = [col[1] for col in COLUMNS_TO_BE_RENAMED_ON24_ATTENDEE_QUESTIONS]
    # temp fix , to be removed when country cleanup in place.
    country = country.withColumnRenamed('country_id', 'ISO3'). \
        withColumnRenamed('iso2', 'ISO2')
    # ------------------------------------- Veeva Surveys -------------------- #
    survey = survey.filter(F.col('row_is_current') == True)

    # Renaming columns in order to be merged
    for columns in COLUMNS_TO_BE_RENAMED_SURVEY:
        survey = survey.withColumnRenamed(columns[0], columns[1])
    # updating as Glo , otherwise records will be removed in standardize_country
    survey = survey.withColumn('country_id', F.coalesce(F.col('country_id'), F.lit('Glo')))
    survey = standardize_country_code(survey, country)
    # Select only a subset of columns
    survey = survey. \
        select(*COLUMNS_TO_BE_SELECTED_SURVEY). \
        withColumn('channels', F.regexp_replace("channels", "_vod", "")). \
        withColumn('assignment_type', F.regexp_replace("assignment_type", "_vod", "")). \
        withColumn('is_survey_expired', F.when(
        F.upper(F.col("is_survey_expired")) == 'YES', F.lit(True))
                   .otherwise(F.lit(False))). \
        withColumn(
        "contact_reason_id",
        F.concat_ws(
            '_',
            *[F.col("country_id"),
              F.col("contact_reason_id")]
        )
    )
    # clean the contact_reason_id column
    survey = survey.withColumn(
        "contact_reason_id",
        F.when(
            F.col('contact_reason_id').contains('_'),
            F.col('contact_reason_id')
        ).otherwise(
            F.lit(None)
        )
    )
    survey = survey.withColumn("territories", F.when(
        F.col("territory_id").isNotNull(),
        F.array(unicode_normalize("territory_id"))
    ).otherwise(
        F.split(
            unicode_normalize(F.regexp_replace("territories", "^;", "")),
            ";"
        )
    )).drop('territory_id')
    survey = survey.withColumn('source', F.lit('Veeva'))
    survey = survey.withColumn('is_polls', F.lit(False))
    survey = get_record_type_details(record_type, survey)

    survey_question = survey_question.filter(F.col('row_is_current') == True)
    # Renaming columns in order to be merged
    for columns in COLUMNS_TO_BE_RENAMED_SURVEY_QUEST:
        survey_question = survey_question.withColumnRenamed(columns[0], columns[1])
    survey_question = survey_question.withColumn(
        'answer_choices', clean_answer_choices('answer_choices')
    ).withColumn(
        'answer_choices', F.when(
            F.col("answer_choices").isNull(),
            None
        ).otherwise(F.array_join(F.col("answer_choices"), "; "))
    )
    # Select only a subset of columns
    survey_question = survey_question. \
        select(*COLUMNS_TO_BE_SELECTED_SURVEY_QUEST). \
        dropDuplicates()
    survey_question = get_record_type_details(record_type, survey_question). \
        withColumnRenamed('description', 'question_description'). \
        withColumnRenamed('record_type', 'question_record_type')

    survey_responses = survey_responses.filter(F.col('row_is_current') == True)
    survey_responses = survey_responses.withColumn(
        'number', F.col('number').cast('int')
    )
    # Renaming columns in order to be merged
    for columns in COLUMNS_TO_BE_RENAMED_SURVEY_RESPONSE:
        survey_responses = survey_responses.withColumnRenamed(columns[0], columns[1])
    # Select only a subset of columns
    survey_responses = survey_responses. \
        select(*COLUMNS_TO_BE_SELECTED_SURVEY_RESPONSE). \
        dropDuplicates()

    survey_responses = get_record_type_details(record_type, survey_responses). \
        withColumnRenamed('description', 'response_description'). \
        withColumnRenamed('record_type', 'response_record_type')

    survey_responses = survey_responses.withColumn('res_create_date', F.to_date("response_create_date"))
    # Renaming columns in order to be merged
    for columns in COLUMNS_TO_BE_RENAMED_ONE_EMP_TO_MANY_TERR:
        one_emp_to_many_terr = one_emp_to_many_terr.withColumnRenamed(columns[0], columns[1])

    # Select only a subset of columns
    one_emp_to_many_terr = one_emp_to_many_terr.filter(
        (F.col('response_collected_veeva_territory_id') != 'NoTerritory') & (F.col('employee_id') != 'NoEmployee')) \
        .select(*COLUMNS_TO_BE_SELECTED_ONE_EMP_TO_MANY_TERR). \
        dropDuplicates()

    one_emp_to_many_terr = one_emp_to_many_terr.withColumn(
        'response_end_date', F.when(
            F.year(F.col('response_end_date')) == end_year,
            F.lit(F.current_date())) \
            .otherwise(F.col('response_end_date')))

    # territory corresponding to the employee who has collected the survey response at point of time of the response date
    survey_responses = survey_responses \
        .join(
        F.broadcast(one_emp_to_many_terr),
        on=[
            survey_responses.employee_id == one_emp_to_many_terr.employee_id,
            (survey_responses.res_create_date >= one_emp_to_many_terr.response_start_date) &
            (survey_responses.res_create_date <= one_emp_to_many_terr.response_end_date)
        ],
        how="left"
    ).drop(
        one_emp_to_many_terr.employee_id
    ).drop(
        one_emp_to_many_terr.response_start_date
    ).drop(
        one_emp_to_many_terr.response_end_date
    ).drop(
        survey_responses.res_create_date
    )

    survey_targets = survey_targets.filter(F.col('row_is_current') == True)

    for columns in COLUMNS_TO_BE_RENAMED_SURVEY_TARGETS:
        survey_targets = survey_targets.withColumnRenamed(columns[0], columns[1])
    survey_targets = survey_targets.withColumn('is_not_completed', F.col('is_not_completed').cast('boolean'))
    # Select only a subset of columns
    survey_targets = survey_targets. \
        select(*COLUMNS_TO_BE_SELECTED_SURVEY_TARGETS). \
        dropDuplicates()
    survey_targets = survey_targets. \
        withColumn('veeva_account_id', F.col('stakeholder_id'))
    survey_targets = add_in_stakeholder_id(survey_targets, stakeholder). \
        drop('source_account_id', 'source_account')
    survey_targets = get_record_type_details(record_type, survey_targets). \
        withColumnRenamed('description', 'target_description'). \
        withColumnRenamed('record_type', 'target_record_type')
    # filter last 3 yrs data
    surveys = survey_responses. \
        join(survey_question, 'survey_question_code', 'left'). \
        join(survey_targets, 'survey_target_code', 'left'). \
        join(survey, 'survey_code', 'left'). \
        withColumn('country_id', F.when(F.col('country_id') == 'Glo',
                                        F.coalesce('survey_response_country_id', F.lit('Glo'))).otherwise(
        F.col('country_id'))). \
        filter(F.col('country_id').isNotNull()). \
        filter(F.year(F.col('end_date')) >= F.lit(F.year(F.current_date()) - 3)). \
        drop('survey_response_country_id')
    surveys = surveys.withColumn('response_date', F.to_date('response_date'))
    surveys = surveys.withColumn(
        "survey_id",
        F.concat_ws(
            '_',
            *[F.col("survey_code"),
              F.col("survey_question_code"),
              F.col("survey_response_code"),
              F.col("survey_target_code")]
        )
    )
    surveys = surveys.withColumn('is_survey', F.lit(False))
    # veeva surveys ends

    # on24 survey
    on24_survey = on24_survey.filter(F.col('country').isNotNull()). \
        withColumnRenamed('country', 'country_name'). \
        join(
        country.withColumnRenamed('ISO3', 'country_id'),
        'country_name',
        'left'
    )
    on24_survey = on24_survey. \
        withColumn('survey_response_code', F.concat_ws(
        '_', *[F.col('survey_id'), F.col('survey_question_id'), F.col('event_user_id')]
    ))

    for columns in COLUMNS_TO_BE_RENAMED_ON24_SURVEY:
        on24_survey = on24_survey.withColumnRenamed(columns[0], columns[1])

    on24_survey = on24_survey. \
        select(*COLUMNS_TO_BE_SELECTED_ON24_SURVEY).dropDuplicates()
    on24_survey = on24_survey.filter(F.col('country_id').isNotNull())
    on24_survey = on24_survey.withColumn('survey_question_name', F.col('survey_question_code'))
    on24_survey_question_choices = on24_survey.groupBy(
        'country_id',
        'survey_code',
        'survey_question_code'
    ).agg(
        F.concat_ws(';', F.collect_set('answer_choice_response')).alias('answer_choices')
    )
    on24_survey = on24_survey. \
        join(on24_survey_question_choices, ['country_id', 'survey_code', 'survey_question_code'])
    on24_survey = on24_survey.withColumn(
        "survey_id",
        F.col('survey_response_code')
    )
    # responses given by the use for the question
    on24_survey = on24_survey.sort(F.col('row_is_current').desc())
    on24_survey = on24_survey.groupBy(
        ON24_SURVEY_GRP_BY_COLS
    ).agg(
        F.concat_ws(';', F.collect_set('survey_response_name')).alias('survey_response_name'),
        F.concat_ws(';', F.collect_set('free_text_response')).alias('free_text_response'),
        F.concat_ws(';', F.collect_set('answer_choice_response')).alias('answer_choice_response'),
        F.max('row_last_modified').alias('row_last_modified'),
        F.first('row_is_current').alias('row_is_current')
    )
    on24_survey = on24_survey.withColumn(
        'contact_id',
        F.concat_ws(
            '_',
            F.coalesce(
                F.col('country_id'),
                F.lit('')
            ),
            F.coalesce(
                F.col('event_id'),
                F.lit('')
            ),
            F.coalesce(
                F.col('event_user_id'),
                F.lit('')
            )
        )
    ).distinct()
    webinar = webinar.select('contact_id', 'stakeholder_id', 'veeva_account_id').dropDuplicates()
    on24_survey = on24_survey.join(webinar, 'contact_id', 'left').drop('event_id', 'event_user_id', 'contact_id')
    on24_survey = on24_survey.withColumn('source', F.lit('ON24'))
    on24_survey = on24_survey.withColumn('is_polls', F.lit(False))
    on24_survey = on24_survey.withColumn('is_survey', F.lit(True))
    # on24 survey ends

    # on24 polls
    on24_polls = on24_polls.filter(F.col('country').isNotNull()). \
        withColumnRenamed('country', 'country_name'). \
        join(
        country.withColumnRenamed('ISO3', 'country_id'),
        'country_name',
        'left'
    )
    on24_polls = on24_polls. \
        withColumn('survey_response_code', F.concat_ws(
        '_', *[F.col('poll_id'), F.col('poll_question_id'), F.col('event_user_id')]
    ))

    for columns in COLUMNS_TO_BE_RENAMED_ON24_POLLS:
        on24_polls = on24_polls.withColumnRenamed(columns[0], columns[1])

    on24_polls = on24_polls. \
        select(*COLUMNS_TO_BE_SELECTED_ON24_POLLS).dropDuplicates()
    on24_polls = on24_polls.filter(F.col('country_id').isNotNull())
    on24_polls = on24_polls.withColumn('survey_question_name', F.col('survey_question_code'))
    on24_polls_question_choices = on24_polls.groupBy(
        'country_id',
        'survey_code',
        'survey_question_code'
    ).agg(
        F.concat_ws(';', F.collect_set('answer_choice_response')).alias('answer_choices')
    )
    on24_polls = on24_polls. \
        join(on24_polls_question_choices, ['country_id', 'survey_code', 'survey_question_code'])
    on24_polls = on24_polls.withColumn(
        "survey_id",
        F.col('survey_response_code')
    )
    # responses given by the use for the question
    on24_polls = on24_polls.sort(F.col('row_is_current').desc())
    on24_polls = on24_polls.groupBy(
        ON24_SURVEY_GRP_BY_COLS
    ).agg(
        F.concat_ws(';', F.collect_set('survey_response_name')).alias('survey_response_name'),
        F.concat_ws(';', F.collect_set('free_text_response')).alias('free_text_response'),
        F.concat_ws(';', F.collect_set('answer_choice_response')).alias('answer_choice_response'),
        F.max('row_last_modified').alias('row_last_modified'),
        F.first('row_is_current').alias('row_is_current')
    )
    on24_polls = on24_polls.withColumn(
        'contact_id',
        F.concat_ws(
            '_',
            F.coalesce(
                F.col('country_id'),
                F.lit('')
            ),
            F.coalesce(
                F.col('event_id'),
                F.lit('')
            ),
            F.coalesce(
                F.col('event_user_id'),
                F.lit('')
            )
        )
    ).distinct()
    on24_polls = on24_polls.join(webinar, 'contact_id', 'left').drop('event_id', 'event_user_id', 'contact_id')
    on24_polls = on24_polls.withColumn('source', F.lit('ON24'))
    on24_polls = on24_polls.withColumn('is_polls', F.lit(True))
    on24_polls = on24_polls.withColumn('is_survey', F.lit(False))
    on24_surveys = safe_union(on24_survey, on24_polls)
    on24_surveys = on24_surveys.withColumn('response_date_time', F.col('response_date'))
    on24_surveys = on24_surveys.withColumn('response_date', F.to_date('response_date'))
    # on24 attendee_questions
    attendee_questions = attendee_questions.filter(F.col('country').isNotNull()). \
        withColumnRenamed('country', 'country_name'). \
        join(
        country.withColumnRenamed('ISO3', 'country_id'),
        'country_name',
        'left'
    )
    for columns in COLUMNS_TO_BE_RENAMED_ON24_ATTENDEE_QUESTIONS:
        attendee_questions = attendee_questions.withColumnRenamed(columns[0], columns[1])

    attendee_questions = attendee_questions. \
        select(*COLUMNS_TO_BE_SELECTED_ON24_ATTENDEE_QUESTIONS).dropDuplicates()
    attendee_questions = attendee_questions. \
        withColumn('survey_response_code', F.concat_ws(
        '_', *[F.col('event_id'), F.col('event_user_id'), F.col('survey_question_code'), F.col('answer_presenter_id')]
    ))

    attendee_questions = attendee_questions.filter(F.col('country_id').isNotNull())
    attendee_questions = attendee_questions.withColumn('survey_question_name', F.col('survey_question_code'))
    attendee_questions = attendee_questions.withColumn('survey_code', F.col('event_id'))
    attendee_questions = attendee_questions.withColumn('internal_id', F.col('event_user_id'))
    attendee_questions = attendee_questions.withColumn(
        "survey_id",
        F.col('survey_response_code')
    )

    attendee_questions = attendee_questions.withColumn(
        'contact_id',
        F.concat_ws(
            '_',
            F.coalesce(
                F.col('country_id'),
                F.lit('')
            ),
            F.coalesce(
                F.col('event_id'),
                F.lit('')
            ),
            F.coalesce(
                F.col('event_user_id'),
                F.lit('')
            )
        )
    ).distinct()
    webinar = webinar.select('contact_id', 'stakeholder_id', 'veeva_account_id').dropDuplicates()
    attendee_questions = attendee_questions.join(webinar, 'contact_id', 'left').drop('event_id', 'event_user_id',
                                                                                     'contact_id')
    attendee_questions = attendee_questions.withColumn('source', F.lit('ON24'))
    attendee_questions = attendee_questions.withColumn('is_polls', F.lit(False))
    attendee_questions = attendee_questions.withColumn('is_questions', F.lit(True))
    attendee_questions = attendee_questions.withColumn('is_survey', F.lit(False))

    # on24 attendee_questions ends
    surveys = safe_union(surveys, on24_surveys)
    surveys = safe_union(surveys, attendee_questions)
    surveys = surveys.withColumn(
        "survey_id",
        F.concat_ws(
            '_',
            *[F.col("survey_id"),
              F.col("response_collected_veeva_territory_id")]
        )
    )
    surveys = get_gps_details(surveys, country.withColumnRenamed('ISO3', 'country_id'))
    surveys = gps_division(surveys, 'survey')
    return surveys.distinct()


def suggestions(**kwargs):
    suggestions = kwargs['suggestions']
    suggestions_feed = kwargs['suggestions_feed']
    stakeholder = kwargs['stakeholder']
    country = kwargs['country']
    record_type = kwargs['record_type']

    COLUMNS_TO_BE_RENAMED_SUGGESTIONS = {
        "account_id": "veeva_account_id",
        "account_priority_score": "stakeholder_priority_score",
        "category": "category",
        "is_display_dismiss": "is_dismissed",
        "no_homepage": "is_on_home_page",
        "expiration_date": "expire_date",
        "is_display_complete": "is_complete",
        "planned_call_date": "planned_contact_date",
        "posted_date": "posted_date",
        "priority": "priority",
        "reason": "reason",
        "record_type_name": "suggestion_record_type",
        "display_score": "is_score_on_home_page",
        "suggestion_external_id": "external_id",
        "suppress_reason": "is_reason_on_stakeholder_panel_page",
        "title": "title",
        "owner_id": "owner_id",
        "abv_expiry_reason": "expiry_reason",
        "country_id": "country_id",
        "id": "suggestions_id",
        "row_is_current": "row_is_current",
        "row_last_modified": "row_last_modified",
        "created_date": "created_date",
        "name": "suggestions_name"
    }
    COLUMNS_TO_BE_SELECTED_SUGGESTIONS = [
        "veeva_account_id",
        "stakeholder_priority_score",
        "category",
        "is_dismissed",
        "is_on_home_page",
        "expire_date",
        "is_complete",
        "planned_contact_date",
        "posted_date",
        "priority",
        "reason",
        "suggestion_record_type",
        "is_score_on_home_page",
        "external_id",
        "is_reason_on_stakeholder_panel_page",
        "title",
        "owner_id",
        "expiry_reason",
        "country_id",
        "suggestions_id",
        "row_is_current",
        "row_last_modified",
        "created_date",
        "suggestions_name"
    ]

    COLUMNS_TO_BE_RENAMED_SUGGESTIONS_FEED = {
        "activity_execution_type": "exceution_type",
        "dismiss_feedback_1": "is_feedback_dismissed_1",
        "dismiss_feedback_2": "is_feedback_dismissed_2",
        "dismiss_feedback_3": "is_feedback_dismissed_3",
        "dismiss_feedback_4": "is_feedback_dismissed_4",
        "suggestion": "suggestions_id",
        "last_modified_by_id": "last_modifed_by",
        "created_by_id": "created_by",
        "name": "suggestions_feedback_name"
    }
    COLUMNS_TO_BE_SELECTED_SUGGESTIONS_FEED = [
        'call_id',
        'exceution_type',
        'is_feedback_dismissed_1',
        'is_feedback_dismissed_2',
        'is_feedback_dismissed_3',
        'is_feedback_dismissed_4',
        'sent_email',
        'suggestions_feedback_name',
        'last_modifed_by',
        'created_by',
        'record_type_id',
        'last_modified_date',
        'suggestions_id'
    ]

    # Renaming columns in order to be merged
    for curr_name, reqd_name in COLUMNS_TO_BE_RENAMED_SUGGESTIONS.items():
        suggestions = suggestions.withColumnRenamed(curr_name, reqd_name)

    for curr_name, reqd_name in COLUMNS_TO_BE_RENAMED_SUGGESTIONS_FEED.items():
        suggestions_feed = suggestions_feed.withColumnRenamed(curr_name, reqd_name)

    # Select only a subset of columns
    suggestions = suggestions.select(*COLUMNS_TO_BE_SELECTED_SUGGESTIONS).dropDuplicates()
    suggestions_feed = suggestions_feed.select(*COLUMNS_TO_BE_SELECTED_SUGGESTIONS_FEED)

    # suggestion Header has one child record.
    # wierd case for suggestion a4b070000004HrvAAE in suggestion_feed, hence picking the latest feed
    sfeed_window = Window.partitionBy('suggestions_id') \
        .orderBy(suggestions_feed.last_modified_date.desc())
    suggestions_feed = suggestions_feed \
        .withColumn('row_number', F.row_number().over(sfeed_window)) \
        .filter(F.col('row_number') == 1) \
        .drop('row_number', 'last_modified_date')
    suggestions = suggestions.join(suggestions_feed, 'suggestions_id', 'left')

    # transformation to clean up data
    pattern = '%true%'
    suggestions = suggestions.withColumn('suggestion_record_type', F.upper('suggestion_record_type'))
    convert_to_boolean_fields = {
        "is_feedback_dismissed_1": "is_feedback_dismissed_1",
        "is_feedback_dismissed_2": "is_feedback_dismissed_2",
        "is_feedback_dismissed_3": "is_feedback_dismissed_3",
        "is_feedback_dismissed_4": "is_feedback_dismissed_4",
    }
    for curr_name, reqd_name in convert_to_boolean_fields.items():
        suggestions = suggestions \
            .withColumn(curr_name, F.when(F.lower(curr_name).like(pattern), True).otherwise(False))
    # there could multiple stakeholder_id, this is safe to do to avoid duplicates on stakeholder
    stakeholder = stakeholder.select('stakeholder_id', 'veeva_account_id')
    stakeholder_window = Window.partitionBy('veeva_account_id').orderBy(stakeholder.stakeholder_id)
    stakeholder = stakeholder \
        .withColumn('row_number', F.row_number().over(stakeholder_window)) \
        .filter(F.col('row_number') == 1) \
        .drop('row_number')
    suggestions = suggestions \
        .withColumn('contact_id', F.when(
        F.col('suggestion_record_type').like('%CALL_VOD%'),
        F.col('call_id')
    ).when(
        ((F.col('suggestion_record_type').like('%EMAIL_VOD%')) & (F.col('sent_email').isNotNull())),
        F.concat_ws('_', F.col('country_id'), F.col('sent_email'))
    ).otherwise(
        F.lit(None))
                    ).drop('sent_email', 'call_id')
    suggestions = suggestions.join(stakeholder, 'veeva_account_id', 'left')
    suggestions = get_record_type_details(record_type, suggestions)
    suggestions = get_gps_details(suggestions, country.select('country_id', 'affiliate'))
    suggestions = gps_division(suggestions, 'suggestions')
    return suggestions


def dynamic_market_share(**kwargs):
    rheumatology = kwargs['rheumatology_projected']
    gastroenterology = kwargs['gastroenterology_projected']
    dermatology = kwargs['dermatology_projected']

    column_renames = {
        "overall": "overall",
        "first_line": "first_line_of_therapy",
        "second_line_1": "second_line_of_therapy_1",
        "second_line_2": "second_line_of_therapy_2",
        "third_line": "third_line_of_therapy",
        "country": "country_id"
    }
    columns_to_select = [
        "country_id",
        "brand",
        "indication",
        "molecule",
        "date",
        "overall",
        "first_line_of_therapy",
        "second_line_of_therapy_1",
        "second_line_of_therapy_2",
        "third_line_of_therapy",
        "dynamic_initiation",
        "dynamic_overall",
        "dynamic_switch",
        "row_is_current",
        "row_last_modified"]
    for curr_name, reqd_name in column_renames.items():
        rheumatology = rheumatology.withColumnRenamed(curr_name, reqd_name)

    # deriving the start of the month for that qurter and brand info
    rheumatology = rheumatology.withColumn('month', F.when(
        F.col('quarter') == 1, F.lit('01')).when(F.col('quarter') == 2, F.lit('04')).
                                           when(F.col('quarter') == 3, F.lit('07')).when(F.col('quarter') == 4,
                                                                                         F.lit('10')))

    rheumatology = rheumatology.withColumn('date',
                                           F.to_date(F.concat_ws('-', F.col('year'), F.col('month'), F.lit('01')))). \
        withColumn('brand', F.upper(F.split('drug', ' ')[0])). \
        withColumn('molecule', F.split(F.regexp_replace(F.regexp_replace('drug', '\\(', ''), '\\)', ''), ' ')[1]). \
        select(columns_to_select)

    for curr_name, reqd_name in column_renames.items():
        gastroenterology = gastroenterology.withColumnRenamed(curr_name, reqd_name)
    # deriving the start of the month for that qurter and brand info
    gastroenterology = gastroenterology.withColumn('month', F.when(
        F.col('quarter') == 1, F.lit('01')).when(F.col('quarter') == 2, F.lit('04')).
                                                   when(F.col('quarter') == 3, F.lit('07')).when(F.col('quarter') == 4,
                                                                                                 F.lit('10')))

    gastroenterology = gastroenterology.withColumn('date', F.to_date(
        F.concat_ws('-', F.col('year'), F.col('month'), F.lit('01')))). \
        withColumn('brand', F.upper(F.split('drug', ' ')[0])). \
        withColumn('molecule', F.split(F.regexp_replace(F.regexp_replace('drug', '\\(', ''), '\\)', ''), ' ')[1]). \
        select(columns_to_select)

    for curr_name, reqd_name in column_renames.items():
        dermatology = dermatology.withColumnRenamed(curr_name, reqd_name)
    # deriving the start of the month for that qurter and brand info
    dermatology = dermatology.withColumn('month', F.when(
        F.col('quarter') == 1, F.lit('01')).when(F.col('quarter') == 2, F.lit('04')).
                                         when(F.col('quarter') == 3, F.lit('07')).when(F.col('quarter') == 4,
                                                                                       F.lit('10')))

    dermatology = dermatology.withColumn('date',
                                         F.to_date(F.concat_ws('-', F.col('year'), F.col('month'), F.lit('01')))). \
        withColumn('brand', F.upper(F.split('drug', ' ')[0])). \
        withColumn('molecule', F.split(F.regexp_replace(F.regexp_replace('drug', '\\(', ''), '\\)', ''), ' ')[1]). \
        select(columns_to_select)

    df = safe_union(rheumatology, gastroenterology)
    df = safe_union(df, dermatology)
    df = df.withColumn('molecule',
                       F.when(F.col('molecule').like('%bios%'), F.lit('')).otherwise(F.upper(F.col('molecule'))))
    df = df.withColumn('dynamic_market_share_id', F.sha2(
        F.concat_ws("_", *['country_id', 'date', 'brand', 'indication']), 256))
    update_window = Window.partitionBy('dynamic_market_share_id').orderBy(df.date)

    df = df.withColumn('overall', F.sum('overall').over(update_window)). \
        withColumn('first_line_of_therapy', F.sum('first_line_of_therapy').over(update_window)). \
        withColumn('second_line_of_therapy_1', F.sum('second_line_of_therapy_1').over(update_window)). \
        withColumn('second_line_of_therapy_2', F.sum('second_line_of_therapy_2').over(update_window)). \
        withColumn('dynamic_initiation', F.sum('dynamic_initiation').over(update_window)). \
        withColumn('third_line_of_therapy', F.sum('third_line_of_therapy').over(update_window)). \
        withColumn('dynamic_switch', F.sum('dynamic_switch').over(update_window)). \
        withColumn('dynamic_overall', F.sum('dynamic_overall').over(update_window)). \
        withColumn('row_last_modified', F.max('row_last_modified').over(update_window)). \
        withColumn('row_is_current', F.first('row_is_current').over(update_window))
    return df.dropDuplicates()


def holidays(**kwargs):
    holidaylist = kwargs['holidaylist']
    column_renames = {
        "activity_date": "date",
        "activity_type": "reason"
    }
    columns_to_select = [
        "country_id",
        'area',
        'affiliate',
        'source',
        'date',
        'reason'
    ]
    for curr_name, reqd_name in column_renames.items():
        holidaylist = holidaylist.withColumnRenamed(curr_name, reqd_name)
    holidays = holidaylist.filter(F.col('country_id').isNotNull()) \
        .select(columns_to_select)
    # Removed 'National Day for Truth and Reconciliation', 'Easter Monday' holidays for Canada
    holidays = holidays. \
        withColumn('holiday_removal', F.when(
        (F.col('country_id') == 'CAN') &
        (F.col('source') == 'ADW') &
        (F.col('reason').isin(['National Day for Truth and Reconciliation', 'Easter Monday'])) &
        (F.col('date').isin(['2022-09-30', '2022-04-18'])), F.lit(False)).otherwise(True))

    holidays = holidays.filter(F.col('holiday_removal') == True).drop('holiday_removal')

    return holidays


def stakeholder_pref(**kwargs):
    stakeholder_pref = kwargs['stakeholder_pref']
    country = kwargs['country']

    COLUMNS_TO_BE_RENAMED_STAKEHOLDER_PREF = [
        ('account_id', 'veeva_account_id'),
        ("id", "preference_id"),
        ('type', 'preference_type'),
        ('value', 'preference_value'),
        ('create_date', 'preference_created_date'),
        ('created_by_id', 'preference_created_by'),
        ('last_modified_date', 'preference_last_modified_date'),
        ('last_modified_by_id', 'preference_last_modified_by'),
        ('country', 'country_id')
    ]
    COLUMNS_TO_BE_SELECTED_STAKEHOLDER_PREF = ["row_is_current", "row_last_modified"]
    for col in COLUMNS_TO_BE_RENAMED_STAKEHOLDER_PREF:
        stakeholder_pref = stakeholder_pref.withColumnRenamed(col[0], col[1])
        COLUMNS_TO_BE_SELECTED_STAKEHOLDER_PREF.append(col[1])
    stakeholder_pref = stakeholder_pref.select(*COLUMNS_TO_BE_SELECTED_STAKEHOLDER_PREF)
    stakeholder_pref = get_gps_details(stakeholder_pref, country.select('country_id', 'affiliate'))

    return stakeholder_pref


def journey(**kwargs):
    schema = T.StructType([
        T.StructField('Journey Name', T.StringType()),
        T.StructField('CLM', T.ArrayType(T.LongType())),
        T.StructField('VAE', T.ArrayType(T.LongType())),
        T.StructField('Abbvie pro', T.ArrayType(T.StringType())),
        T.StructField('Mass Email', T.ArrayType(T.StringType())),
        T.StructField('Goal status', T.StringType()),
    ])

    # TODO: might need to remove if data source has a correct json format
    def parse_json(string):
        if string is None:
            string = ''
        ignore_quotes = "'(.*)'"
        match = re.match(ignore_quotes, string)
        prefix = "Journey_parameter1="
        if match is not None:
            string = match.group(1)
        elif string.startswith("{") and string.endswith("'"):
            string = string[:-1]
        elif string.startswith(prefix):
            string = string.split(prefix)[-1][:-1]
        try:
            parsed_json = json.loads(string)
        except Exception:
            # return an empty valid json to prevent runtime exception
            parsed_json = {}
        return {
            'Journey Name': parsed_json.get('Journey Name'),
            'CLM': parsed_json.get('CLM'),
            'VAE': parsed_json.get('VAE'),
            'Abbvie pro': parsed_json.get('Abbvie pro'),
            'Mass Email': parsed_json.get('Mass Email'),
            'Goal status': parsed_json.get('Goal status')
        }

    parse_json_udf = F.udf(parse_json, schema)
    country = kwargs['country'].select("iso2", "country_id", "affiliate")
    stakeholder = (kwargs['stakeholder'].filter(F.lower(F.col("source")).isin(["veeva", "reltio"]))
                   .select("veeva_account_id", "stakeholder_id")
                   )

    columns_map = {
        "journey_id": "journey_code",
        "event_date": "event_time",
        "journey_name": "name",
        "journey_version": "version",
        "journey_parameter_1": "journey_parameter",
        "event_parameter_1": "event_parameter",
        "stakeholder_id": "veeva_account_id"
    }
    division_condition = F.col("journey_code").like("%_Journey_M%")
    has_journey_exit = (F.when(F.col("event_type") == 'Journey_exit', True)
                        .when(F.col("event_type").isNull(), F.lit(None))
                        .otherwise(False)
                        )
    # closed_loop_marketing_content_id from content
    sfmc_journey_logs = (rename_columns(kwargs['sfmc_journey_logs'], columns_map)
                         .withColumn('iso2', F.substring(F.col("journey_code"), 1, 2))
                         .withColumn('event_date', F.to_date(F.col('event_time')))
                         .withColumn('json', parse_json_udf(F.col("journey_parameter")))
                         .withColumn('journey_name', F.col('json.`Journey Name`'))
                         .withColumn('closed_loop_marketing_vault_document_id', F.col('json.`CLM`'))
                         .withColumn('veeva_approved_email_vault_document_id', F.col('json.`VAE`'))
                         .withColumn('goal_status', F.col('json.`Goal status`'))
                         .withColumn('has_journey_exit', has_journey_exit)
                         )
    content = (kwargs['content'].filter(F.col("content_type") == "Key Message")
               .filter(F.col("vault_document_id").isNotNull())
               .select("vault_document_id", "content_id")
               .distinct()
               .withColumn("vault_document_id", F.col("vault_document_id").cast(T.LongType()))
               )
    content_ids_df = (sfmc_journey_logs.select("closed_loop_marketing_vault_document_id")
                      .distinct()
                      .join(content,
                            F.array_contains(sfmc_journey_logs.closed_loop_marketing_vault_document_id,
                                             content.vault_document_id),
                            "left")
                      .select("closed_loop_marketing_vault_document_id", "content_id")
                      .distinct()
                      .groupBy("closed_loop_marketing_vault_document_id")
                      .agg(F.collect_list("content_id").alias("closed_loop_marketing_content_id"))
                      .select("closed_loop_marketing_vault_document_id", "closed_loop_marketing_content_id")
                      )
    df = (sfmc_journey_logs.join(F.broadcast(country), "iso2", "left")
          .join(content_ids_df, "closed_loop_marketing_vault_document_id", "left")
          .join(stakeholder, "veeva_account_id", "left")
          .withColumn('journey_id', hash_into_key(
        ["journey_code", F.coalesce("stakeholder_id", "veeva_account_id"), "event_type", "event_date"]))
          )
    columns_of_array_type = (
        "closed_loop_marketing_vault_document_id",
        "closed_loop_marketing_content_id",
        "veeva_approved_email_vault_document_id")

    for column_name in columns_of_array_type:
        df = empty_arrays_to_null(df, column_name)

    df = add_division(df, division_condition)
    return df.select(
        "country_id",
        "journey_code",
        "journey_id",
        "event_time",
        "event_date",
        "veeva_account_id",
        "stakeholder_id",
        "event_type",
        "journey_parameter",
        "journey_name",
        "closed_loop_marketing_vault_document_id",
        # "closed_loop_marketing_content_id",
        "veeva_approved_email_vault_document_id",
        "goal_status",
        "has_journey_exit",
        "affiliate",
        "division"
    )


def medical_tier(**kwargs):
    medical_tiers = kwargs['medical_tierr'],
    country = kwargs['country']

    medical_tiers = medical_tiers[0].select("*")
    medical_tiers = medical_tiers.withColumnRenamed("country", "country_id")
    return get_gps_details(medical_tiers, country.select('country_id', 'affiliate'))