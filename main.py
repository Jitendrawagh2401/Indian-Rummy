import streamlit as st
import pandas as pd


def main():
    global df, uploaded_file, pivot_table_df, get_last_valid_record, add_event_category_column, highlight_paid_ad_impression, total_unique_users, create_merged_table, count_unique_users_paid_ad, create_combined_pivot_table, get_last_5_records, get_app_remove_and_non_app_remove_users
    st.markdown(
            """
            <style>
            .stApp {
                background-image: url('data:image/jpeg;base64,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');            
                background-size: cover; /* Adjusts the size to cover the entire container */
                background-repeat: no-repeat;
                background-attachment: fixed;
                background-position: center;
            }
            </style>
            """,
            unsafe_allow_html=True
        )
    st.sidebar.markdown(
            """
            <div style="text-align: center; padding: 10px;">
                <img src="data:image/jpeg;base64,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" style="max-width: 100%; height: auto;">
            </div>
            """,
            unsafe_allow_html=True
        )
    # File uploader for CSV
    uploaded_file = st.sidebar.file_uploader("Upload your event data (CSV only)", type=["csv"])

    if uploaded_file is None:
        st.title("Analysis of Indian Rummy Data 📉")
    else:
        # Show a different title after data is uploaded
        st.title("Analysis of User Removal and Churn Events in the Indian Rummy Game 🃏")

        # Read the CSV file
        df = pd.read_csv(uploaded_file)
        total_unique_users = df['user_pseudo_id'].nunique()
        st.sidebar.header('Total Users')
        st.sidebar.write(f"Total Users: {total_unique_users}")

        with st.spinner('Please wait, results are being processed...'):

            # Convert 'event_time_UTC' to datetime and create 'event_date' column
            df['event_time_IST'] = pd.to_datetime(df['event_time_IST'], errors='coerce', utc=True)
            df['event_date'] = df['event_time_IST'].dt.date
            last_record_df = get_last_valid_record(df)

            # Add the event category column directly into the original data
            df = add_event_category_column(df, last_record_df)
            # Display original data
            st.write("Original Data:")
            st.dataframe(df)


            unique_category = df['event_category'].unique()
            selected_category = st.sidebar.multiselect("Select Event category", options=unique_category)
            if selected_category:
                df = df[df['event_category'].isin(selected_category)]
            unique_dates = df['event_date'].unique()
            selected_dates = st.sidebar.multiselect("Select Event Date", options=unique_dates)
            if selected_dates:
                df = df[df['event_date'].isin(selected_dates)]


            st.success('Results are ready😀!')


            # Get the last valid record for each user
            last_record_df = get_last_valid_record(df)

            # Display the last valid record table
            st.subheader("Last Valid Record for Each User")
            st.dataframe(last_record_df)


            app_remove_last5_df = get_last_5_records(df, last_record_df, app_remove=True)
            non_app_remove_last5_df = get_last_5_records(df, last_record_df, app_remove=False)

            app_remove_df = get_app_remove_and_non_app_remove_users(df, last_record_df, app_remove=True)

            # Display data for Non-App Remove users
            non_app_remove_df = get_app_remove_and_non_app_remove_users(df, last_record_df, app_remove=False)


            pivot_table_df = create_combined_pivot_table(last_record_df)

            #st.write("Summary of User Counts (Vertical with Percentage):")
            #st.dataframe(pivot_table_df.style.format({"Percentage": "{:.2f}%"}))
            app_remove_paid_count = count_unique_users_paid_ad(app_remove_last5_df)
            non_app_remove_paid_count = count_unique_users_paid_ad(non_app_remove_last5_df)

            pivot_table_df['Ad Impression'] = [app_remove_paid_count, non_app_remove_paid_count]
            pivot_table_df['Ad%'] = (pivot_table_df['Ad Impression'] / pivot_table_df['User Count']) * 100
            # Display the merged table
            st.subheader("User Removal and Churn Summary")
            st.dataframe(pivot_table_df.style.format({"Percentage": "{:.2f}%", "Ad%": "{:.2f}%"}))

            pivot_table_df = create_combined_pivot_table(last_record_df)
            # Create merged retention and app removed table
            merged_table_df = create_merged_table(df, last_record_df, total_unique_users)

            # Display merged retention and app removed table
            st.subheader("Date-wise Unique User Retention and App Removed Users")
            st.dataframe(merged_table_df.style.format({"Retention Percentage": "{:.2f}%", "App Removed Percentage": "{:.2f}%"}))

            st.subheader("Last 5 Records for App Remove Users")

            # Apply the highlighting to the DataFrame
            app_remove_last5_df_styled = app_remove_last5_df.style.apply(highlight_paid_ad_impression, axis=1)

            # Display the styled DataFrame
            st.dataframe(app_remove_last5_df_styled)

            # Display the last 5 records for non-app_remove users
            st.subheader("Last 5 Records for Churn Users")

            # Apply the highlighting to the DataFrame
            non_app_remove_last5_df_styled = non_app_remove_last5_df.style.apply(highlight_paid_ad_impression, axis=1)

            # Display the styled DataFrame
            st.dataframe(non_app_remove_last5_df_styled)

            #st.subheader("App Remove Users Data")
            #st.dataframe(filtered_df)

            #st.subheader("Non-App Remove Users Data")
            #st.dataframe(filtered_df)


            session_start_df = df[df['event_name'] == 'session_start']
            session_counts = session_start_df.groupby('user_pseudo_id').size()
            session_count_pivot = session_counts.value_counts().reset_index(name='Total users')
            session_count_pivot.columns = ['Session Count', 'Total users']
            session_count_pivot = session_count_pivot.sort_values(by='Session Count')
            session_count_pivot['Percentage'] = (session_count_pivot['Total users'] /session_count_pivot['Total users'].sum()) * 100

            st.subheader("Pivot Table of Session by Total Users:")
            st.write(session_count_pivot.style.format({'Percentage': "{:.2f}%"}))
            single_session_users = session_counts[session_counts == 1].index

            # Step 5: Filter the original data for these users
            single_session_df = df[df['user_pseudo_id'].isin(single_session_users)]

            # Step 6: Display the filtered data in Streamlit
            #st.subheader("Data for Users with Exactly One Session:")
            #st.dataframe(single_session_df)

            single_session_df = df[df['user_pseudo_id'].isin(single_session_users)]  # From the earlier filtered single session users

            # Step 2: Define specific events for win/loss analysis
            specific_events = [
                'andbah_game_lost',
                'andbah_game_won',
                'call_break_lost',
                'call_break_win',
                'IR_2_player_game_loss',
                'IR_2_player_game_won',
                'IR_3_player_game_loss',
                'IR_3_player_game_won',
                'IR_4_player_game_loss',
                'IR_4_player_game_won',
                'IR_5_player_game_loss',
                'IR_5_player_game_won',
                'ludo_bot_game_lost',
                'ludo_bot_game_won',
            ]

            # Step 3: Filter the single session data for the specific win/loss events
            filtered_events_df = single_session_df[single_session_df['event_name'].isin(specific_events)]

            # Step 4: Count occurrences of each event
            event_counts = filtered_events_df['event_name'].value_counts().reset_index()
            event_counts.columns = ['event_name', 'count']

            # Step 5: Calculate total counts for each game type (win + loss)
            total_counts = {
                'IR_2_player_game_loss': event_counts[event_counts['event_name'] == 'IR_2_player_game_loss']['count'].values[0] + \
                                         event_counts[event_counts['event_name'] == 'IR_2_player_game_won']['count'].values[0],
                'IR_2_player_game_won': event_counts[event_counts['event_name'] == 'IR_2_player_game_loss']['count'].values[0] + \
                                        event_counts[event_counts['event_name'] == 'IR_2_player_game_won']['count'].values[0],  # Include loss count
                'IR_3_player_game_loss': event_counts[event_counts['event_name'] == 'IR_3_player_game_loss']['count'].values[0] + \
                                         event_counts[event_counts['event_name'] == 'IR_3_player_game_won']['count'].values[0],
                'IR_3_player_game_won': event_counts[event_counts['event_name'] == 'IR_3_player_game_loss']['count'].values[0] + \
                                        event_counts[event_counts['event_name'] == 'IR_3_player_game_won']['count'].values[0],  # Include loss count
                'IR_4_player_game_loss': event_counts[event_counts['event_name'] == 'IR_4_player_game_loss']['count'].values[0] + \
                                         event_counts[event_counts['event_name'] == 'IR_4_player_game_won']['count'].values[0],
                'IR_4_player_game_won': event_counts[event_counts['event_name'] == 'IR_4_player_game_loss']['count'].values[0] + \
                                        event_counts[event_counts['event_name'] == 'IR_4_player_game_won']['count'].values[0],  # Include loss count
                'IR_5_player_game_loss': event_counts[event_counts['event_name'] == 'IR_5_player_game_loss']['count'].values[0] + \
                                         event_counts[event_counts['event_name'] == 'IR_5_player_game_won']['count'].values[0],
                'IR_5_player_game_won': event_counts[event_counts['event_name'] == 'IR_5_player_game_loss']['count'].values[0] + \
                                        event_counts[event_counts['event_name'] == 'IR_5_player_game_won']['count'].values[0],  # Include loss count
                'andbah_game_lost': event_counts[event_counts['event_name'] == 'andbah_game_lost']['count'].values[0] + \
                                    event_counts[event_counts['event_name'] == 'andbah_game_won']['count'].values[0],
                'andbah_game_won': event_counts[event_counts['event_name'] == 'andbah_game_lost']['count'].values[0] + \
                                   event_counts[event_counts['event_name'] == 'andbah_game_won']['count'].values[0],  # Include loss count
                'ludo_bot_game_lost': event_counts[event_counts['event_name'] == 'ludo_bot_game_lost']['count'].values[0] + \
                                      event_counts[event_counts['event_name'] == 'ludo_bot_game_won']['count'].values[0],
                'ludo_bot_game_won': event_counts[event_counts['event_name'] == 'ludo_bot_game_lost']['count'].values[0] + \
                                     event_counts[event_counts['event_name'] == 'ludo_bot_game_won']['count'].values[0],  # Include loss count
                'call_break_lost': event_counts[event_counts['event_name'] == 'call_break_lost']['count'].values[0] + \
                                   event_counts[event_counts['event_name'] == 'call_break_win']['count'].values[0],
                'call_break_win': event_counts[event_counts['event_name'] == 'call_break_lost']['count'].values[0] + \
                                  event_counts[event_counts['event_name'] == 'call_break_win']['count'].values[0],  # Include loss count
            }

            # Step 6: Create DataFrame for total counts
            total_counts_df = pd.DataFrame(total_counts.items(), columns=['event_name', 'total_count'])

            # Step 7: Merge event counts with total counts
            combined_counts = pd.merge(event_counts, total_counts_df, on='event_name', how='outer')

            # Step 8: Calculate percentage
            combined_counts['percentage'] = combined_counts.apply(
                lambda row: f"{(row['count'] / total_counts[row['event_name']] * 100):.0f}%" if row['event_name'] in total_counts and total_counts[row['event_name']] > 0 else '', axis=1
            )

            # Step 9: Hide total_count for won events (for better display)
            combined_counts['total_count'] = combined_counts.apply(
                lambda row: row['total_count'] if row['event_name'] in ['IR_2_player_game_loss', 'IR_3_player_game_loss', 'IR_4_player_game_loss', 'IR_5_player_game_loss', 'andbah_game_lost', 'ludo_bot_game_lost', 'call_break_lost'] else '', axis=1
            )

            # Step 10: Sort the combined counts by event_name
            combined_counts = combined_counts.sort_values(by='event_name')

            # Step 11: Display the results in Streamlit
            st.subheader("Win/Loss Ratio for Users with Exactly One Session")
            st.dataframe(combined_counts)

            # Return the filtered DataFrame for further use if needed
            #return combined_counts


            # Optionally, if you want to return this filtered DataFrame
            #return single_session_df
            def categorize_event(event_name):
                if 'IR_2_player' in event_name:
                    return 'IR_2_player'
                elif 'IR_3_player' in event_name:
                    return 'IR_3_player'
                elif 'IR_4_player' in event_name:
                    return 'IR_4_player'
                elif 'IR_5_player' in event_name:
                    return 'IR_5_player'
                else:
                    return event_name  # return the original name for unique events

            # Define the first set of specific events for counting occurrences
            specific_events_1 = [
                'andbah_game_start',
                'IR_2_player_game_start',
                'IR_3_player_game_start',
                'IR_4_player_game_start',
                'IR_5_player_game_start',
                'ludo_bot_game_start',
                'ludo_local_game_start'
            ]

            # Filter the main data to only include rows where event_name is in the first specific event list
            filtered_df1 = df[df['event_name'].isin(specific_events_1)]

            # Create a new column for categorized events in the first filtered dataframe
            filtered_df1['Categorized Event'] = filtered_df1['event_name'].apply(categorize_event)

            # Create a pivot table to count the occurrences of each categorized event
            event_count_pivot1 = filtered_df1.pivot_table(
                index='Categorized Event',
                values='user_pseudo_id',
                aggfunc='count',
                fill_value=0
            ).reset_index()

            # Rename columns for clarity
            event_count_pivot1.columns = ['Event Name', 'game start users']

            # Display the first pivot table
            #st.subheader("Event Count Pivot Table (Game Start Events)")
            #st.dataframe(event_count_pivot1)

            # Define the second set of specific events for counting distinct users
            specific_events_2 = [
                'andbah_game_start',
                'IR_2_player_game_played',
                'IR_3_player_game_played',
                'IR_4_player_game_played',
                'IR_5_player_game_played',
                'ludo_bot_game_start',
                'ludo_local_game_start'
            ]

            # Filter the main data to only include rows where event_name is in the second specific event list
            filtered_df2 = df[df['event_name'].isin(specific_events_2)]

            # Create a new column for categorized events in the second filtered dataframe
            filtered_df2['Categorized Event'] = filtered_df2['event_name'].apply(categorize_event)

            # Create a pivot table to count the occurrences of each categorized event based on unique users
            event_count_pivot2 = filtered_df2.pivot_table(
                index='Categorized Event',
                values='user_pseudo_id',
                aggfunc='nunique',
                fill_value=0
            ).reset_index()

            # Rename columns for clarity
            event_count_pivot2.columns = ['Event Name', 'Total Users played']

            # Display the second pivot table
            #st.subheader("Event Count Pivot Table (Game Played Events)")
            #st.dataframe(event_count_pivot2)

            # Merge the two pivot tables on the 'Event Name' column
            merged_event_count = pd.merge(event_count_pivot1, event_count_pivot2, on='Event Name', how='outer')

            # Rename the columns for clarity after merging
            merged_event_count.columns = ['Event Name', 'game start users', 'Total Users played']

            # Calculate the percentage column
            merged_event_count['Avg.play game'] = merged_event_count['game start users'] / merged_event_count['Total Users played']

            # Display the merged table with percentage
            st.subheader("Avg. games played per Users:")
            st.write(merged_event_count.style.format({'Percentage': "{:.2f}"}))


            def categorize_event(event_name):
                            if 'IR_2_player' in event_name:
                                return 'IR_2_player'
                            elif 'IR_3_player' in event_name:
                                return 'IR_3_player'
                            elif 'IR_4_player' in event_name:
                                return 'IR_4_player'
                            elif 'IR_5_player' in event_name:
                                return 'IR_5_player'
                            else:
                                return event_name  # return the original name for unique events

                        # Define the first set of specific events for counting occurrences
            specific_events_1 = [
                            'IR_2_player_game_start',
                            'IR_3_player_game_start',
                            'IR_4_player_game_start',
                            'IR_5_player_game_start',
                        ]

                        # Filter the main data to only include rows where event_name is in the first specific event list
            filtered_df1 = df[df['event_name'].isin(specific_events_1)]

                        # Create a new column for categorized events in the first filtered dataframe
            filtered_df1['Categorized Event'] = filtered_df1['event_name'].apply(categorize_event)

                        # Create a pivot table to count the occurrences of each categorized event
            event_count_pivot1 = filtered_df1.pivot_table(
                            index='Categorized Event',
                            values='user_pseudo_id',
                            aggfunc='nunique',
                            fill_value=0
                        ).reset_index()

                        # Rename columns for clarity
            event_count_pivot1.columns = ['Event Name', 'start game continue']

                        # Display the first pivot table
                        #st.subheader("Event Count Pivot Table (Game Start Events)")
                        #st.dataframe(event_count_pivot1)

                        # Define the second set of specific events for counting distinct users
            specific_events_2 = [
                            'IR_2_player_game_played',
                            'IR_3_player_game_played',
                            'IR_4_player_game_played',
                            'IR_5_player_game_played',
                        ]

                        # Filter the main data to only include rows where event_name is in the second specific event list
            filtered_df2 = df[df['event_name'].isin(specific_events_2)]

                        # Create a new column for categorized events in the second filtered dataframe
            filtered_df2['Categorized Event'] = filtered_df2['event_name'].apply(categorize_event)

                        # Create a pivot table to count the occurrences of each categorized event based on unique users
            event_count_pivot2 = filtered_df2.pivot_table(
                            index='Categorized Event',
                            values='user_pseudo_id',
                            aggfunc='nunique',
                            fill_value=0
                        ).reset_index()

                        # Rename columns for clarity
            event_count_pivot2.columns = ['Event Name', 'first time play']

                        # Display the second pivot table
                        #st.subheader("Event Count Pivot Table (Game Played Events)")
                        #st.dataframe(event_count_pivot2)

                        # Merge the two pivot tables on the 'Event Name' column
            merged_event_count = pd.merge(event_count_pivot1, event_count_pivot2, on='Event Name', how='outer')

                        # Rename the columns for clarity after merging
            merged_event_count.columns = ['Event Name', 'start game continue', 'first time play']

                        # Calculate the percentage column
            merged_event_count['Percentage'] = merged_event_count['start game continue'] / merged_event_count['first time play']*100

                        # Display the merged table with percentage
            st.subheader("continue playes games users:")
            st.write(merged_event_count.style.format({'Percentage': "{:.2f}%"}))


def get_last_valid_record(df):
            """
            Function to get the last valid record for each user based on the sorted timestamp.
            """
            last_records = df.groupby('user_pseudo_id').last().reset_index()
            return last_records
def create_combined_pivot_table(last_record_df):
            """
            Create a combined pivot table that summarizes app removed users and churn users.
            """
            # Categorize users as 'App Removed' or 'Churned'
            last_record_df['Status'] = last_record_df['event_name'].apply(
                lambda x: 'App Removed' if x == 'app_remove' else 'Churned'
            )

            # Create a pivot table counting occurrences of each status
            pivot_table = last_record_df.pivot_table(
                index='Status',
                values='user_pseudo_id',
                aggfunc='count',
                fill_value=0
            ).reset_index()

            # Rename columns for clarity
            pivot_table.columns = ['Status', 'User Count']

            # Calculate total users for percentage calculation
            total_users = pivot_table['User Count'].sum()

            # Calculate the percentage of each status
            pivot_table['Percentage'] = (pivot_table['User Count'] / total_users) * 100

            return pivot_table
def create_merged_table(df, last_record_df, total_unique_users):
            """
            Create a merged table for date-wise unique user retention and app removed users.
            """
            # Step 1: Create the retention table (unique users per date)
            retention_counts = df.groupby('event_date')['user_pseudo_id'].nunique().reset_index()
            retention_counts.columns = ['Date', 'Unique Users']

            # Calculate retention percentage based on total unique users
            retention_counts['Retention Percentage'] = (retention_counts['Unique Users'] / total_unique_users) * 100

            # Step 2: Filter for app removed users and create a table for app removed users per date
            app_removed_df = last_record_df[last_record_df['event_name'] == 'app_remove']
            app_removed_counts = app_removed_df.groupby('event_date')['user_pseudo_id'].nunique().reset_index()
            app_removed_counts.columns = ['Date', 'App Removed Users']

            # Step 3: Merge the retention table with the app removed table on 'Date'
            merged_df = pd.merge(retention_counts, app_removed_counts, on='Date', how='left')

            merged_df['App Removed Users'].fillna(0, inplace=True)
            merged_df['App Removed Users'] = merged_df['App Removed Users'].astype(int)

            # Step 4: Calculate the app removed percentage based on the unique users for each date
            merged_df['App Removed Percentage'] = (merged_df['App Removed Users'] / merged_df['Unique Users']) * 100

            return merged_df
def get_last_5_records(df, last_record_df, app_remove=True):
            """
            Function to retrieve the last 5 records for users based on their last valid event.
            - If app_remove=True, fetch the last 5 records for app_remove users.
            - If app_remove=False, fetch the last 5 records for non-app_remove users.
            """
            if app_remove:
                # Filter for users whose last valid event is 'app_remove'
                app_remove_users = last_record_df[last_record_df['event_name'] == 'app_remove']['user_pseudo_id'].unique()
                filtered_df = df[df['user_pseudo_id'].isin(app_remove_users)]
            else:
                # Filter for users whose last valid event is NOT 'app_remove'
                non_app_remove_users = last_record_df[last_record_df['event_name'] != 'app_remove']['user_pseudo_id'].unique()
                filtered_df = df[df['user_pseudo_id'].isin(non_app_remove_users)]

            # Sort by user and timestamp
            filtered_df = filtered_df.sort_values(by=['user_pseudo_id', 'event_time_UTC'], ascending=[True, False])

            # Get the last 5 records for each user
            last_5_records = filtered_df.groupby('user_pseudo_id').head(5).reset_index(drop=True)

            return last_5_records

def get_app_remove_and_non_app_remove_users(df, last_record_df, app_remove=True):
            """
            Function to retrieve all records for users based on their last valid event.
            - If app_remove=True, fetch all records for app_remove users.
            - If app_remove=False, fetch all records for non-app_remove users.
            """
            if app_remove:
                # Filter for users whose last valid event is 'app_remove'
                app_remove_users = last_record_df[last_record_df['event_name'] == 'app_remove']['user_pseudo_id'].unique()
                filtered_df = df[df['user_pseudo_id'].isin(app_remove_users)]
                st.subheader("App Remove Users Data")
                st.dataframe(filtered_df)
            else:
                # Filter for users whose last valid event is NOT 'app_remove'
                non_app_remove_users = last_record_df[last_record_df['event_name'] != 'app_remove']['user_pseudo_id'].unique()
                filtered_df = df[df['user_pseudo_id'].isin(non_app_remove_users)]
                #st.subheader("Non-App Remove Users Data")
                #st.dataframe(filtered_df)

            # Sort by user and timestamp for easier inspection
            filtered_df = filtered_df.sort_values(by=['user_pseudo_id', 'event_time_UTC'], ascending=[True, False])

            specific_events = [
                    'andbah_game_lost',
                    'andbah_game_won',
                    'call_break_lost',
                    'call_break_win',
                    'IR_2_player_game_loss',
                    'IR_2_player_game_won',
                    'IR_3_player_game_loss',
                    'IR_3_player_game_won',
                    'IR_4_player_game_loss',
                    'IR_4_player_game_won',
                    'IR_5_player_game_loss',
                    'IR_5_player_game_won',
                    'ludo_bot_game_lost',
                    'ludo_bot_game_won',
                ]

            filtered_events_df = filtered_df[filtered_df['event_name'].isin(specific_events)]
            event_counts = filtered_events_df['event_name'].value_counts().reset_index()
            event_counts.columns = ['event_name', 'count']

            # Sort the pivot table by event_name


            total_counts = {
            'IR_2_player_game_loss': event_counts[event_counts['event_name'] == 'IR_2_player_game_loss']['count'].values[0] + \
                                     event_counts[event_counts['event_name'] == 'IR_2_player_game_won']['count'].values[0],
            'IR_2_player_game_won': event_counts[event_counts['event_name'] == 'IR_2_player_game_loss']['count'].values[0] + \
                                    event_counts[event_counts['event_name'] == 'IR_2_player_game_won']['count'].values[0],  # Include loss count
            'IR_3_player_game_loss': event_counts[event_counts['event_name'] == 'IR_3_player_game_loss']['count'].values[0] + \
                                     event_counts[event_counts['event_name'] == 'IR_3_player_game_won']['count'].values[0],
            'IR_3_player_game_won': event_counts[event_counts['event_name'] == 'IR_3_player_game_loss']['count'].values[0] + \
                                    event_counts[event_counts['event_name'] == 'IR_3_player_game_won']['count'].values[0],  # Include loss count
            'IR_4_player_game_loss': event_counts[event_counts['event_name'] == 'IR_4_player_game_loss']['count'].values[0] + \
                                     event_counts[event_counts['event_name'] == 'IR_4_player_game_won']['count'].values[0],
            'IR_4_player_game_won': event_counts[event_counts['event_name'] == 'IR_4_player_game_loss']['count'].values[0] + \
                                    event_counts[event_counts['event_name'] == 'IR_4_player_game_won']['count'].values[0],  # Include loss count
            'IR_5_player_game_loss': event_counts[event_counts['event_name'] == 'IR_5_player_game_loss']['count'].values[0] + \
                                     event_counts[event_counts['event_name'] == 'IR_5_player_game_won']['count'].values[0],
            'IR_5_player_game_won': event_counts[event_counts['event_name'] == 'IR_5_player_game_loss']['count'].values[0] + \
                                    event_counts[event_counts['event_name'] == 'IR_5_player_game_won']['count'].values[0],  # Include loss count
            'andbah_game_lost': event_counts[event_counts['event_name'] == 'andbah_game_lost']['count'].values[0] + \
                                event_counts[event_counts['event_name'] == 'andbah_game_won']['count'].values[0],
            'andbah_game_won': event_counts[event_counts['event_name'] == 'andbah_game_lost']['count'].values[0] + \
                               event_counts[event_counts['event_name'] == 'andbah_game_won']['count'].values[0],  # Include loss count
            'ludo_bot_game_lost': event_counts[event_counts['event_name'] == 'ludo_bot_game_lost']['count'].values[0] + \
                                event_counts[event_counts['event_name'] == 'ludo_bot_game_won']['count'].values[0],
            'ludo_bot_game_won': event_counts[event_counts['event_name'] == 'ludo_bot_game_lost']['count'].values[0] + \
                                 event_counts[event_counts['event_name'] == 'ludo_bot_game_won']['count'].values[0],  # Include loss count
            'call_break_lost': event_counts[event_counts['event_name'] == 'call_break_lost']['count'].values[0] + \
                                event_counts[event_counts['event_name'] == 'call_break_win']['count'].values[0],
            'call_break_win': event_counts[event_counts['event_name'] == 'call_break_lost']['count'].values[0] + \
                             event_counts[event_counts['event_name'] == 'call_break_win']['count'].values[0],  # Include loss count
            }

            # Adding total counts to the event_counts DataFrame
            total_counts_df = pd.DataFrame(total_counts.items(), columns=['event_name', 'total_count'])

            # Merge event counts with total counts
            combined_counts = pd.merge(event_counts, total_counts_df, on='event_name', how='outer')

            # Calculate percentage
            combined_counts['percentage'] = combined_counts.apply(
                lambda row: f"{(row['count'] / total_counts[row['event_name']] * 100):.0f}%" if row['event_name'] in total_counts and total_counts[row['event_name']] > 0 else '', axis=1
            )

            # Set the total_count for the won events to blank for better display
            combined_counts['total_count'] = combined_counts.apply(
                lambda row: row['total_count'] if row['event_name'] in ['IR_2_player_game_loss', 'IR_3_player_game_loss', 'IR_4_player_game_loss', 'IR_5_player_game_loss', 'andbah_game_lost', 'ludo_bot_game_lost', 'call_break_lost'] else '', axis=1
            )

            # Sort the combined counts by event_name
            combined_counts = combined_counts.sort_values(by='event_name')

            # Display in Streamlit
            if app_remove:
                st.subheader("Win/Loss Ratio for App Remove Users")
            else:
                st.subheader("Win/Loss Ratio for Churn Users")

            st.dataframe(combined_counts)


            return filtered_df


def count_unique_users_paid_ad(df):
            """
            Function to count unique users who have the 'paid_ad_impression' event in the last 5 records.
            """
            paid_ad_users = df[df['event_name'] == 'paid_ad_impression']['user_pseudo_id'].nunique()
            return paid_ad_users

def highlight_paid_ad_impression(row):
            """
            Function to apply background color to rows where event_name is 'paid_ad_impression'.
            """
            return ['background-color: lightgreen' if row['event_name'] == 'paid_ad_impression' else '' for _ in row]


def add_event_category_column(main_df, last_record_df):
                    # Merge the main data with the last valid record table to determine the last event for each user
                    merged_df = pd.merge(main_df, last_record_df, on='user_pseudo_id', how='left', suffixes=('', '_last'))

                    # Create a new column 'event_category' based on the last event
                    merged_df['event_category'] = merged_df['event_name_last'].apply(lambda x: 'app_remove' if x == 'app_remove' else 'non_app_remove')

                    return merged_df



if __name__ == "__main__":
    main()
