

library(tidyverse)

X08_12 <- Mobile_Dental_Bookings_2025_08_12 %>%
  filter(str_detect(`Location Name`,regex('Goleta|GO'))) %>%
  distinct(`MRN`,.keep_all = TRUE) %>%
  view()


X08_14 <- Mobile_Dental_Bookings_2025_08_14 %>%
  filter(str_detect(`Location Name`,regex('Goleta|GO'))) %>%
  distinct(`MRN`,.keep_all = TRUE) %>%
  view()



Bound <- bind_rows(X08_12,X08_14) %>%
  view()



# --- Prepare Base Dataset ---
SBNC_Outreach_ <- X08_14 %>%
  mutate(
    DOB  = ymd(DOB),
    Name = str_squish(str_to_upper(`Full Patient Name`))
  ) %>%
  separate(
    col = Name,
    into = c("Last Name", "First Name"),
    sep = ",\\s*",          # allow optional space after comma
    remove = FALSE,         # keep the full Name too
    extra  = "merge",
    fill   = "right"
  ) %>%
  mutate(
    Location = recode(
      `Location Name`,
      'Eastside - ICC Dental'             = 'Eastside Family Dental Clinic',
      'Eastside Family Dental - Outreach' = 'Eastside Family Dental Clinic',
      'GO Neighborhood Dental Clinic OLD' = 'Goleta Neighborhood Dental Clinic',
      'Goleta Smile Van'                  = 'Goleta Neighborhood Dental Clinic',
      'Z Eastside Family Dental Clinic'   = 'Eastside Family Dental Clinic',
      .default = `Location Name`, .missing = `Location Name`
    ),
    Language = recode(
      Language,
      'Spanish; Castilian' = 'Spanish',
      .default = Language, .missing = Language
    )
  )





# Reformat for outreach
cleaned <- SBNC_Outreach_ %>%
  mutate(
    personLastName = `Last Name`,
    personMidName = NA,
    personFirstName = `First Name`,
    personCellPhone = `Phone Number`,
    personHomePhone = NA,
    personWorkPhone = NA,
    personPrefLanguage = `Language`,
    dob = format(DOB, "%Y%m%d"),
    gender = `Sex at Birth`,
    personID = MRN,
    PersonEmail = Email
  ) %>%
  select(
    personLastName, personMidName, personFirstName,
    personCellPhone, personHomePhone, personWorkPhone,
    personPrefLanguage, dob, gender, personID, PersonEmail
  ) %>%
  drop_na(personCellPhone)



# Save each cleaned outreach list as a CSV file by location
write.csv(cleaned, file = 'GNC_Tuesday_Outreach_20250814.csv', row.names = FALSE)


