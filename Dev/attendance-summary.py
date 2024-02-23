import nest_asyncio
nest_asyncio.apply()

import aiohttp
import asyncio
import json
import pandas as pd
from datetime import datetime 
from datetime import timedelta

ums_base_url = "https://app.edoofa.com/version-test/api/1.1/obj"
ums_bearer_token = "786720e8eb68de7054d1149b56cc04f9"
ums_headers = {'Authorization': f'Bearer {ums_bearer_token}'}

async def fetch_table_data(session, base_url, headers, table, constraints=None):
    records = []
    cursor = 0
    total_fetched = 0

    while True:
        params = {'limit': 100, 'cursor': cursor}
        if constraints:
            params['constraints'] = json.dumps(constraints)

        api_url = f"{base_url}/{table}"
        print(f"Fetching {table} data from {base_url}... Cursor: {cursor}")

        async with session.get(api_url, headers=headers, params=params) as response:
            if response.status != 200:
                print(f"Failed to fetch data from {table}: {await response.text()}")
                break

            data = await response.json()
            new_records = data['response']['results']
            records.extend(new_records)
            total_fetched += len(new_records)

            print(f"Fetched {len(new_records)} new records, Total fetched: {total_fetched}")

            cursor += 100

            if len(new_records) < 100:
                print(f"Exiting loop, fetched less than 100 records.")
                break

    df = pd.DataFrame(records)
    print(f"Fetched {len(df)} records for {table}.")
    return df

def get_percentage(number_of_present, total_sessions):
    if total_sessions == 0:
        return 0  
    percent = (number_of_present / total_sessions) * 100
    return percent

async def check_if_row_exists(student, year, month, ums_avg_att_df):
    existing_rows = ums_avg_att_df[
        (ums_avg_att_df['admissions-group-name'] == student) &
        (ums_avg_att_df['year'] == year) &
        (ums_avg_att_df['month'] == month)
    ]

    if not existing_rows.empty:
        return existing_rows.iloc[0]  # Return the first matching row
    else:
        return None

async def post_new_attendance_summary(session, base_url, headers, data):
    api_url = f"{base_url}/Attendance-Summary"
    async with session.post(api_url, headers=headers, json=data) as response:
        if response.status == 201:
            print("Successfully created a new row in Attendance-Summary.")
        else:
            print(f"Failed to create a new row: {await response.text()}")

async def patch_attendance_summary(session, base_url, headers, row_id, data):
    api_url = f"{base_url}/Attendance-Summary/{row_id}"
    async with session.patch(api_url, headers=headers, json=data) as response:
        if response.status == 200:
            print("Successfully updated the row in Attendance-Summary.")
        else:
            print(f"Failed to update the row: {await response.text()}")

async def process_att_summary(session, base_url, headers, ums_att_df, ums_avg_att_df):
    ums_att_df['date'] = pd.to_datetime(ums_att_df['date'])
    unique_students = ums_att_df['admissions-group-name'].unique()
    earliest_date = ums_att_df['date'].min()
    latest_date = ums_att_df['date'].max()

    for student in unique_students:
        # Fetch the ewyl-group-name for the current student
        # Assuming ewyl-group-name is consistent for each student
        ewyl_group_name = ums_att_df[ums_att_df['admissions-group-name'] == student]['ewyl-group-name'].iloc[0]

        current_month = earliest_date.replace(day=1)

        while current_month <= latest_date:
            # All sessions for this student up to the current month (excluding the current month)
            previous_sessions_df = ums_att_df[
                (ums_att_df['admissions-group-name'] == student) &
                (ums_att_df['date'] < current_month)
            ]

            # Calculate average attendance percentage till last month
            total_sessions_till_last_month = len(previous_sessions_df)
            attended_sessions_till_last_month = previous_sessions_df['present'].sum()
            avg_att_percent_till_last_month = get_percentage(attended_sessions_till_last_month, total_sessions_till_last_month)

            # Sessions for this student in the current month
            this_month_df = ums_att_df[
                (ums_att_df['admissions-group-name'] == student) &
                (ums_att_df['date'].dt.year == current_month.year) &
                (ums_att_df['date'].dt.month == current_month.month)
            ]

            # Calculate attendance percentage for this month
            total_sessions_this_month = len(this_month_df)
            attended_sessions_this_month = this_month_df['present'].sum()
            attendance_percentage_this_month = get_percentage(attended_sessions_this_month, total_sessions_this_month)

            # Check if a summary row already exists for this student and month
            existing_row = await check_if_row_exists(student, current_month.year, current_month.strftime("%B"), ums_avg_att_df)

            # Data for updating or creating a summary row
            data = {
                'admissions-group-name': student,
                'ewyl-group-name': ewyl_group_name,  # Include ewyl-group-name here
                'attendance-percentage': attendance_percentage_this_month,
                'avg-att-percent-till-last-month': avg_att_percent_till_last_month,
                'first-day-of-month': current_month.strftime("%Y-%m-%d"),
                'month': current_month.strftime("%B"),
                'year': current_month.year
            }

            if existing_row is not None:
                # Update the existing row
                await patch_attendance_summary(session, base_url, headers, existing_row['_id'], data)
            else:
                # Create a new summary row
                await post_new_attendance_summary(session, base_url, headers, data)

            # Move to the first day of the next month
            current_month += pd.offsets.MonthBegin()


async def main():
    async with aiohttp.ClientSession() as session:
        ums_att_summary_df = await fetch_table_data(session, ums_base_url, ums_headers, "Attendance-Summary")
        ums_att_df = await fetch_table_data(session, ums_base_url, ums_headers, "Attendance")

        # Convert 'date' columns to datetime
        ums_att_df['date'] = pd.to_datetime(ums_att_df['date'])
        ums_att_summary_df['first-day-of-month'] = pd.to_datetime(ums_att_summary_df['first-day-of-month'])

        await process_att_summary(session, ums_base_url, ums_headers, ums_att_df, ums_att_summary_df)

await main()
