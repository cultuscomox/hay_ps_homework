# install.packages(c("censusapi", "dplyr", "tidyr", "stringr", "vroom"))
library(dplyr)

build_counts <- \(props) {
  props |>
    group_by(year, state_county) |>
    summarise(yrly_new_props = n()) |>
    ungroup() |>
    tidyr::expand(year, state_county) |>
    left_join(yrly_new, join_by(year == year,
                                state_county == state_county)) |>
    mutate(yrly_new_props = tidyr::replace_na(yrly_new_props, 0)) |>
    group_by(state_county) |>
    arrange(year) |>
    mutate(cur_props = cumsum(yrly_new_props))
}

# Wrapper for getCensus, taking named list of census vars, and renaming returned
# vars back to their human-readable names.
.get_rename <- \(name, vars, ...) {
  results <- censusapi::getCensus(name = name,
                                  vars = vars,
                                  ...)
  rename(results, !!! vars)
}

fetch_acs <- \(vintage) {
  .get_rename(name = "acs/acs5",
              vars = acs5_vars,
              vintage = vintage,
              region = "county:*") |>
    mutate(state_county = stringr::str_c(state, county)) |>
    select(-c(state, county)) |>
    rename_with(\(.) stringr::str_glue(".acs_{vintage}_{.}"),
                .cols = - state_county)
}

# Variables to fetch from each survey, and human-readable names.
dec_profile_vars <- list(median_income = "DP3_C150")

decennial_hu_vars <- list(total_pop_occupied = "H003002",
                          total_pop_renter_occupied = "H011003",
                          total_vacant = "H003003",
                          total_pop_occupied = "H010001",
                          total_pop_renter_occupied = "H011D003",
                          total_pop = "P001001",
                          avg_household_size = "H012001",
                          median_age = "P013001")

acs5_vars <- list(total_pop = "B01003_001E",
                  total_pop_occupied = "B07013_002E",
                  total_pop_renter_occupied = "B07013_006E",
                  total_vacant = "B25002_003E",
                  total_same_house_1_year_ago = "B07001_017E",
                  median_income = "B06011_001E", #2010 dollars
                  median_age = "B08103_001E")

# crosswalk to approx match zips to counties
zip_county <- readxl::read_xlsx("ZIP_COUNTY_092021.xlsx") |>
  select(ZIP, COUNTY) |>
  distinct(ZIP, .keep_all = TRUE)

# fips to county names
fips_names <- vroom::vroom("state_and_county_fips_master.csv") |>
  mutate(fips = as.character(fips)) |>
  rename(county_name = name)

# sum properties by year and county
prop_locs <- vroom::vroom("property locations.csv") |>
  inner_join(zip_county, by = join_by(Zip == ZIP)) |>
  rename(state_county = COUNTY) |>
  rename_with(stringr::str_to_lower) |>
  filter(year >= 1975)

# fetch 2000 decennial census data
dec_2000 <- .get_rename(name = "dec/sf1",
                        vars = decennial_hu_vars,
                        vintage = vintage,
                        region = "county:*") |>
  inner_join(.get_rename(name = "dec/sf3profile",
                         vars = dec_profile_vars,
                         vintage = vintage,
                         region = "county:*"),
             join_by(state, county)) |>
  rename_with(\(.) stringr::str_glue(".dec_{vintage}_{.}"),
              .cols = -c(state, county)) |>
  mutate(state_county = stringr::str_c(state, county)) |>
  select(-c(state, county))

# fetch acs data.
acs5_2020 <- fetch_acs(2020)
acs5_2010 <- fetch_acs(2010)

# census data for all counties.
all_counties <- dec_2000 |>
  left_join(acs5_2010, by = "state_county") |>
  left_join(acs5_2020, by = "state_county") |>
  inner_join(fips_names,
             by = join_by(state_county == fips)) |>
  mutate(year = 2023) |>
  relocate(year, .before = everything()) |>
  relocate(year, county_name, state, .before = everything())


# join property counts to census data by county and save.
prop_locs |>
  build_counts() |>
  left_join(all_counties, by = "state_county") |>
  vroom::vroom_write("by_county.csv", delim = ",")

# also save the rest of the county data
vroom::vroom_write(all_counties, "all_counties.csv", delim = ",")
