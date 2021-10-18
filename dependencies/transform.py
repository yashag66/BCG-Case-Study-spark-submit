from pyspark.sql.functions import desc, count, col, dense_rank
from pyspark.sql.window import Window


class Transform:

	def __init__(self):
		pass

	@staticmethod
	def analysis_1_count_crashes_killed_male(primary_person_df):
		"""
		Finds the number of crashes (accidents) in which number of persons killed are male.(Analysis 1)
		param primary_person_df: Spark Dataframe containing primary person data.
		return A dataframe with unique crashes id with number of persons killed as male.
		"""
		num_crashes_male_df = primary_person_df.filter('PRSN_INJRY_SEV_ID="KILLED" AND PRSN_GNDR_ID="MALE"') \
                   .select('CRASH_ID').distinct()

        # Number of crashes where persons killed are male
		print('Number of crashes(accidents) in which number of persons killed are male:', num_crashes_male_df.count())

		# Crash Id's involving males(killed)
		return num_crashes_male_df

	@staticmethod
	def analysis_2_two_wheelers_crashes(units_df):
		"""
		Finds two wheelers which are booked for crashes.
		param units_df: Spark Dataframe containing units data.
		return A dataframe with unique VIN of two wheelers involved in accidents.  
		"""
		# Filtering Units data for two wheelers i.e. 'POLICE MOTORCYCLE' OR 'MOTORCYCLE'
		# Getting unique Vehicle body styles => units_df.select('VEH_BODY_STYL_ID').distinct().collect()
		# Getting unique VIN(vehicle identification number) after applying the above filter.

		two_wheeler_vehicle_body = ['POLICE MOTORCYCLE', 'MOTORCYCLE']

		two_wheelers_crashes_df = units_df.filter(col('VEH_BODY_STYL_ID').isin(two_wheeler_vehicle_body)) \
		                            .select('VIN').filter(col('VIN').isNotNull()).distinct()

		print('Number of two wheelers which are booked for crashes:', two_wheelers_crashes_df.count())

		return two_wheelers_crashes_df

	@staticmethod
	def analysis_3_state_highest_female_accidents(primary_person_df):
		"""
		Finds the state with the highest number of accidents in which females are involved.
		param primary_person_df: Spark Dataframe containing primary person data
		return A dataframe with State ID with count of accidents involving female
		"""
		# Filter accidents involving FEMALE should have PRSN_GNDR_ID set to 'FEMALE'
		# Getting unique vehicle types => primary_person_df.select('PRSN_GNDR_ID').distinct().collect()
		# Assuming the state of accident same as driver licence state

		accident_female_df = primary_person_df.filter('PRSN_GNDR_ID="FEMALE"') \
								.select(['DRVR_LIC_STATE_ID', 'CRASH_ID']).distinct() \
		          				.groupby('DRVR_LIC_STATE_ID').count().orderBy(desc('count'))
		print('Number of crashes(accidents) in which number of persons are female:', \
		          accident_female_df.first()[0])

		return accident_female_df.limit(1)

	@staticmethod
	def analysis_4_top_5_to_15_veh_make_id(primary_person_df, units_df):
		"""
		Finds top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death
		param primary_person_df: Spark Dataframe containing primary person data
		param units_df: Spark Dataframe containing units data.
		return A dataframe with top 5 to 15th Vehicle Make Id's 
		"""
		# List of values pertaining to injury including death
		injury_flag_list = ['KILLED', 'NON-INCAPACITATING INJURY', \
							'POSSIBLE INJURY', 'INCAPACITATING INJURY']

		# Joining Units dataset with primary person dataset to filter only the cashes involving injuries 
		person_units_inner_join_df = primary_person_df.filter(col('PRSN_INJRY_SEV_ID').isin(injury_flag_list)) \
		                            .select('CRASH_ID', 'UNIT_NBR') \
		                            .join(units_df.select(['CRASH_ID', 'UNIT_NBR', 'VEH_MAKE_ID']), ['CRASH_ID', 'UNIT_NBR'], 'inner')

		# Counting unique crashes fo each VEH_MAKE_ID(Eliminating cases where VEH_MAKE_ID is NA)
		person_veh_make_df = person_units_inner_join_df.select('CRASH_ID', 'VEH_MAKE_ID') \
							.filter('VEH_MAKE_ID!="NA"').distinct().groupby('VEH_MAKE_ID') \
							.count().orderBy(desc('count'))

		# Assiging rank based on the crashes/count of each vehicle make
		windowSpec = Window.orderBy(desc('count'))
		person_veh_make_df = person_veh_make_df.withColumn('dense_rank', \
								dense_rank().over(windowSpec))

		top_5_to_15_veh_make_id = person_veh_make_df.filter('dense_rank>=5 AND dense_rank<=15') \
									.select(['VEH_MAKE_ID'])

		print('Top 5th to 15th Veh Make IDs')
		# Filtering top 5th to 15th VEH_MKE_IDs
		top_5_to_15_veh_make_id.show(truncate=False)

		return top_5_to_15_veh_make_id


	@staticmethod
	def analysis_5_top_ethnic_user_group_each_body_style(primary_person_df, units_df):
		"""
		For all the body styles involved in crashes, finds the top ethnic user group of each unique body style
		param primary_person_df: Spark Dataframe containing primary person data
		param units_df: Spark Dataframe containing units data.
		return A dataframe with top ethnic user group for each body style 		
		"""
		# TODO: Add logic to only use required columns not all whle joining two big datasets
		person_units_join_df = primary_person_df.join(units_df, ['CRASH_ID', 'UNIT_NBR'], 'outer')
		# person_units_join_df = primary_person_df.join(units_df, ['CRASH_ID', 'UNIT_NBR'], 'inner')

		# person_units_join_df.select(['PRSN_ETHNICITY_ID', 'VEH_BODY_STYL_ID', 'CRASH_ID', 'UNIT_NBR']).coalesce(1).write.csv('./Hello.csv', header=True)
		body_ethnic_df = person_units_join_df.select(['PRSN_ETHNICITY_ID', 'VEH_BODY_STYL_ID', 'CRASH_ID']) \
		                .filter('VEH_BODY_STYL_ID!="NA" AND VEH_BODY_STYL_ID!="UNKNOWN"') \
		                .distinct().groupby(['VEH_BODY_STYL_ID','PRSN_ETHNICITY_ID']) \
		                .agg(count('CRASH_ID').alias('crash_count'))

		# Using dense rank to find the top ethnicity for each vehicle body styles
		windowSpec = Window.partitionBy('VEH_BODY_STYL_ID').orderBy(desc('crash_count'))
		body_ethnic_df = body_ethnic_df.withColumn('dense_rank', dense_rank().over(windowSpec))

		top_ethnic_user_grp = body_ethnic_df.filter('dense_rank=1') \
								.select(['VEH_BODY_STYL_ID','PRSN_ETHNICITY_ID']) \
		                		.orderBy('VEH_BODY_STYL_ID','PRSN_ETHNICITY_ID')

		print('Top Ethnic Use group for each body style')
		top_ethnic_user_grp.show(truncate=False)

		return top_ethnic_user_grp

	@staticmethod
	def analysis_6_top_5_zip_codes_crash_alcohol(primary_person_df, units_df):
		"""
		Among the crashed cars, finding the Top 5 Zip Codes with highest number crashes with 
		alcohols as the contributing factor to a crash
		param primary_person_df: Spark Dataframe containing primary person data
		param units_df: Spark Dataframe containing units data.
		return A dataframe with top 5 zip codes with crashes due to alcohol
		"""
		# Assuming the below vehichle body styles depict cars
		car_types = ['PASSENGER CAR, 2-DOOR', 'PASSENGER CAR, 4-DOOR', 'POLICE CAR/TRUCK', 
					'SPORT UTILITY VEHICLE', 'NEV-NEIGHBORHOOD ELECTRIC VEHICLE', 'VAN']

		# TODO: Add logic to only use required columns not all whle joining two big datasets
		person_units_join_df = primary_person_df.join(units_df, ['CRASH_ID', 'UNIT_NBR'], 'outer')

		# Filtering for persons with positive alcohol result
		# Filtering for vehicles being car
		# Ignoring records with driver zip not being null
		crashed_car_alcohol_zip_df = person_units_join_df.filter((col('PRSN_ALC_RSLT_ID')=='Positive') \
		                            & (col('DRVR_ZIP').isNotNull()) & (col('VEH_BODY_STYL_ID').isin(car_types))) \
		                            .select(['CRASH_ID', 'DRVR_ZIP', 'VEH_BODY_STYL_ID']).distinct()

		top_5_crashed_car_alcohol = crashed_car_alcohol_zip_df.select(['CRASH_ID', 'DRVR_ZIP']).distinct().groupby('DRVR_ZIP') \
		    .agg(count('CRASH_ID').alias('crash_count')).orderBy(desc('crash_count')).limit(5)

		print('Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash')
		top_5_crashed_car_alcohol.show(truncate=False)

		return top_5_crashed_car_alcohol

	@staticmethod
	def analysis_7_crashes_no_damage_insured_vehicles(units_df, damages_df):
		"""
		Find distinct crash ids where no Damaged Property was observed and Damage Level 
		# is above 4 and car avails Insurance
		param units_df: Spark Dataframe containing units data.
		param damages_df: Spark Dataframe containing damages data
		return A dataframe with distinct crash ids.
		"""
		
		# Damage flag which depict damage level above 4
		damage_flag = ['DAMAGED 5', 'DAMAGED 6', 'DAMAGED 7 HIGHEST']

		# Adding Damages data to Units Data having damage level above 4(Join)
		damages_units_join_df = units_df.filter((col('VEH_DMAG_SCL_1_ID').isin(damage_flag)) | (col('VEH_DMAG_SCL_2_ID').isin(damage_flag))) \
		                        .join(damages_df, ['CRASH_ID'], 'inner')

		# No Damages Flags
		no_damage_property_flag = [row.DAMAGED_PROPERTY \
		                           for row in list(damages_units_join_df.select('DAMAGED_PROPERTY').distinct().collect()) \
		                           if 'NO DAMAGE' in str(row.DAMAGED_PROPERTY).upper()]

		# Assuming the below vehichle body styles depict cars
		car_types = ['PASSENGER CAR, 2-DOOR', 'PASSENGER CAR, 4-DOOR', 'POLICE CAR/TRUCK', 
					'SPORT UTILITY VEHICLE', 'NEV-NEIGHBORHOOD ELECTRIC VEHICLE', 'VAN']

		# List of unique Financial Responsibility Type
		# distinct_fin_res_id = [row.FIN_RESP_TYPE_ID for row in list(units_df.select('FIN_RESP_TYPE_ID').distinct().collect())]
		# List of Responsibility type pertaining to insurance and it's type
		# insurance_flag = ['INSURANCE BINDER', 'LIABILITY INSURANCE POLICY', 'CERTIFICATE OF SELF-INSURANCE',
		#                   'CERTIFICATE OF DEPOSIT WITH COUNTY JUDGE', 'CERTIFICATE OF DEPOSIT WITH COMPTROLLER',
		#                   'SURETY BOND', 'PROOF OF LIABILITY INSURANCE']

		non_insurance_flag = ['NA']

		# Filtering for vehicles which have insurance and the vehicles are of car type
		# Filtering for vehicles which didnt damage property
		distinct_crash_insured_vehicles_damage = damages_units_join_df.filter((~col('FIN_RESP_TYPE_ID').isin(non_insurance_flag)) \
		                & (damages_units_join_df.VEH_BODY_STYL_ID.isin(car_types)) \
		                & (col('DAMAGED_PROPERTY').isin(no_damage_property_flag)) \
		               ).select('CRASH_ID').distinct()

		print("Count of Distinct Crash IDs where No Damaged Property was observed and \
			Damage Level is above 4 and car avails Insurance", distinct_crash_insured_vehicles_damage.count())
		
		return distinct_crash_insured_vehicles_damage

	@staticmethod
	def analysis_8_top_5_vehicle_makers_cd(primary_person_df, units_df, charges_df):
		"""
		Finding the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, 
		uses top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences.
		param primary_person_df: Spark Dataframe containing primary person data
		param units_df: Spark Dataframe containing units data.
		param charges_df: Spark Dataframe containing charges data.
		return A dataframe with top 5 vehicle makers with speeding related offences and additional conditions.

		"""		
		# Speeding related offences flags
		speeding_flag = [row.CHARGE for row in list(charges_df.select('CHARGE').distinct().collect()) if 'SPEED' in str(row.CHARGE).upper()]
		
		# Filtering for car crashes
		car_types = ['PASSENGER CAR, 2-DOOR', 'PASSENGER CAR, 4-DOOR', 'POLICE CAR/TRUCK', 'SPORT UTILITY VEHICLE', 
		             'NEV-NEIGHBORHOOD ELECTRIC VEHICLE', 'VAN']

		# DRIVER LICENSE TYPE
		license_type_id = ['COMMERCIAL DRIVER LIC.', 'OCCUPATIONAL', 'DRIVER LICENSE'] 

		person_units_join_df = primary_person_df.join(units_df, ['CRASH_ID', 'UNIT_NBR'], 'outer')

		# List of 25 states with highest car crashes(Assuming crash can be cosidered as an offense)
		states_highest_car_crash = [row.VEH_LIC_STATE_ID for row in \
									list(units_df.filter(person_units_join_df.VEH_BODY_STYL_ID.isin(car_types))\
									.select(['CRASH_ID', 'VEH_LIC_STATE_ID']).distinct() \
									.groupby('VEH_LIC_STATE_ID').count() \
									.orderBy(desc('count')).limit(25).collect())]

		# Top 10 used vehicle colours
		top_vehicle_colors = [row.VEH_COLOR_ID for row in \
		                      list(units_df.select('CRASH_ID', 'VEH_COLOR_ID').filter('VEH_COLOR_ID!="NA"') \
		                           .groupby('VEH_COLOR_ID').count().orderBy(desc('count')).select('VEH_COLOR_ID') \
		                           .limit(10).collect())]

		top_5_vehicle_makers_df = person_units_join_df.filter((col('VEH_COLOR_ID').isin(top_vehicle_colors)) \
		    & (col('VEH_LIC_STATE_ID').isin(states_highest_car_crash)) \
		    & (col('DRVR_LIC_TYPE_ID').isin(license_type_id))) \
		    .join(charges_df.filter(col('CHARGE').isin(speeding_flag)), ['CRASH_ID', 'UNIT_NBR'], 'inner') \
		    .select(['VEH_MAKE_ID', 'CRASH_ID', 'UNIT_NBR']).distinct() \
		    .groupby('VEH_MAKE_ID').count().orderBy(desc('count')).select('VEH_MAKE_ID').limit(5)

		print('Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, \
		uses top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences.')
		top_5_vehicle_makers_df.show(truncate=False)

		return top_5_vehicle_makers_df 
