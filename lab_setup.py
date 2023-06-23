from faker import Faker
from datetime import date
from datetime import date

## TODO Use logging from other post.

generator = Faker()

def generate_person(id, record_years):
    result = {}
    result['id'] = id
    result['name'] = generator.name()
    result['dob'] = generator.date_of_birth()
    result['record_dt'] = date(2023 - record_years, 1, 1)
    print(f"Generated person {result}")
    return result

def reset_lab(spark):
    print("Resetting Lab")

    print("Step 1. Drop existing tables")
    spark.sql("DROP TABLE IF EXISTS person_info")
    spark.sql("DROP TABLE IF EXISTS person_info_history")

    print("Step 2. Create tables")
    spark.sql(
"""
  CREATE TABLE person_info (
  id BIGINT,
  name STRING,
  dob DATE,
  record_dt DATE
)
""")
    
    print("Step 3. Generate people")
    Faker.seed(1000)

    people = []
    for person_id in range(1,21):
        people.append(generate_person(person_id, int(person_id / 2)))


    print("Step 4. Save people to table")
    df = spark.createDataFrame(people)
    df.write.mode("append").saveAsTable("person_info")
    
