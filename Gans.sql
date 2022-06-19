CREATE DATABASE gans;
USE gans;

###########################################################################################################################################

# SETTING UP CITIES:
# Alter table's datatypes of the columns and set primary key:
ALTER TABLE `gans`.`cities` 
CHANGE COLUMN `city_id` `city_id` VARCHAR(10) NOT NULL ,
CHANGE COLUMN `city` `city` VARCHAR(100) NULL DEFAULT NULL ,
CHANGE COLUMN `country` `country` VARCHAR(100) NULL DEFAULT NULL ,
CHANGE COLUMN `elevation` `elevation` INT NULL DEFAULT NULL ,
ADD PRIMARY KEY (`city_id`);

###########################################################################################################################################

# SETTING UP WEATHER:
# BEFORE altering datatypes and setting keys: Weather has duplicate datetime values; therefore a new column with unique values is necessary:
ALTER TABLE weather 
ADD weather_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY FIRST;

# Alter table's datatypes of the columns and set primary key:
ALTER TABLE `gans`.`weather` 
CHANGE COLUMN `precip_prob` `precip_prob` FLOAT NULL DEFAULT NULL ,
CHANGE COLUMN `temperature` `temperature` FLOAT NULL DEFAULT NULL ,
CHANGE COLUMN `humidity` `humidity` INT NULL DEFAULT NULL ,
CHANGE COLUMN `cloudiness` `cloudiness` INT NULL DEFAULT NULL ,
CHANGE COLUMN `wind_speed` `wind_speed` FLOAT NULL DEFAULT NULL ,
CHANGE COLUMN `wind_gust` `wind_gust` FLOAT NULL DEFAULT NULL ,
CHANGE COLUMN `city` `city` VARCHAR(100) NULL DEFAULT NULL ,
CHANGE COLUMN `city_id` `city_id` VARCHAR(10) NULL DEFAULT NULL ;

# Setting foreign key:
ALTER TABLE `gans`.`weather` 
ADD INDEX `weather_city_id_idx` (`city_id` ASC) VISIBLE;
ALTER TABLE `gans`.`weather` 
ADD CONSTRAINT `weather_city_id`
  FOREIGN KEY (`city_id`)
  REFERENCES `gans`.`cities` (`city_id`)
  ON DELETE NO ACTION
  ON UPDATE NO ACTION;

###########################################################################################################################################

# SETTING UP ARRIVALS:
# BEFORE altering datatypes and setting keys: Arrivals has duplicate datetime values; therefore a new column with unique values is necessary:
ALTER TABLE arrivals 
ADD arrival_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY FIRST;

# Alter table's datatypes of the columns and set primary key:
ALTER TABLE `gans`.`arrivals` 
CHANGE COLUMN `arrival_icao` `arrival_icao` CHAR(4) NULL DEFAULT NULL ,
CHANGE COLUMN `flight_number` `flight_number` VARCHAR(10) NULL DEFAULT NULL ,
CHANGE COLUMN `departure_icao` `departure_icao` CHAR(4) NULL DEFAULT NULL ,
CHANGE COLUMN `departure_airport` `departure_airport` VARCHAR(100) NULL DEFAULT NULL ,
CHANGE COLUMN `scheduled_time` `scheduled_time` DATETIME NULL DEFAULT NULL ,
CHANGE COLUMN `aircraft_model` `aircraft_model` VARCHAR(100) NULL DEFAULT NULL ,
CHANGE COLUMN `airline_name` `airline_name` VARCHAR(100) NULL DEFAULT NULL ;

# Setting foreign key:
ALTER TABLE `gans`.`arrivals` 
ADD INDEX `arrivals_icao_idx` (`arrival_icao` ASC) VISIBLE;
ALTER TABLE `gans`.`arrivals` 
ADD CONSTRAINT `arrivals_icao`
  FOREIGN KEY (`arrival_icao`)
  REFERENCES `gans`.`airports` (`icao`)
  ON DELETE NO ACTION
  ON UPDATE NO ACTION;

###########################################################################################################################################

# SETTING UP AIRPORTS:
# Alter table's datatypes of the columns and set primary key:
ALTER TABLE `gans`.`airports` 
CHANGE COLUMN `icao` `icao` CHAR(4) NOT NULL ,
CHANGE COLUMN `airport_name` `airport_name` VARCHAR(100) NULL DEFAULT NULL ,
CHANGE COLUMN `municipality_name` `municipality_name` VARCHAR(100) NULL DEFAULT NULL ,
CHANGE COLUMN `country_code` `country_code` VARCHAR(5) NULL DEFAULT NULL ,
CHANGE COLUMN `city_id` `city_id` VARCHAR(10) NULL DEFAULT NULL ,
ADD PRIMARY KEY (`icao`);

# Drop localCode column from airports:
ALTER TABLE `gans`.`airports`
DROP COLUMN `localCode`;

# Setting foreign key:
ALTER TABLE `gans`.`airports` 
ADD INDEX `airports_city_id_idx` (`city_id` ASC) VISIBLE;
ALTER TABLE `gans`.`airports` 
ADD CONSTRAINT `airports_city_id`
  FOREIGN KEY (`city_id`)
  REFERENCES `gans`.`cities` (`city_id`)
  ON DELETE NO ACTION
  ON UPDATE NO ACTION;