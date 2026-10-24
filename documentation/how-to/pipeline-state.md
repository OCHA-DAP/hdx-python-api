# Manage Pipeline State

The HDXState class allows the reading and writing of state to a given dataset. Input
and output state transformations can be supplied in read_fn and write_fn
respectively. The input state transformation takes in a string while the output
transformation outputs a string. It is used as follows:

        with temp_dir(folder="test_state") as tmpdir:
            statepath = join(tmpdir, statefile)
            copyfile(join(statefolder, statefile), statepath)
            date1 = datetime(2020, 9, 23, 0, 0, tzinfo=timezone.utc)
            date2 = datetime(2022, 5, 12, 10, 15, tzinfo=timezone.utc)
            with HDXState(
                "test_dataset", tmpdir, parse_date, iso_string_from_datetime
            ) as state:
                assert state.get() == date1
                state.set(date2)
            with HDXState(
                "test_dataset", tmpdir, parse_date, iso_string_from_datetime
            ) as state:
                assert state.get() == date2.replace(hour=0, minute=0)

            with HDXState(
                "test_dataset",
                tmpdir,
                HDXState.dates_str_to_country_date_dict,
                HDXState.country_date_dict_to_dates_str,
            ) as state:
                state_dict = state.get()
                assert state_dict == {"DEFAULT": date1}
                state_dict["AFG"] = date2
                state.set(state_dict)
            with HDXState(
                "test_dataset",
                tmpdir,
                HDXState.dates_str_to_country_date_dict,
                HDXState.country_date_dict_to_dates_str,
            ) as state:
                state_dict = state.get()
                assert state_dict == {
                    "DEFAULT": date1,
                    "AFG": date2.replace(hour=0, minute=0),
                }
