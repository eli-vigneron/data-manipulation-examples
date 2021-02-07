import pandas as pd
import json
import sys

def main():
    try:
        # This file contains the COURSES that a student is taking. Each course has a unique id, a
        # name, and a teacher.
        courses_df = pd.read_csv(sys.argv[1], error_bad_lines=False)

        # This file contains all existing STUDENTS in the database. Each student has a unique id,
        # and a name.
        students_df = pd.read_csv(sys.argv[2], error_bad_lines=False)

        # This file contains all the TESTS each student has received for every test they have written
        tests_df = pd.read_csv(sys.argv[3], error_bad_lines=False)

        # This file contains all the MARKS for each course in the courses.csv file
        marks_df = pd.read_csv(sys.argv[4], error_bad_lines=False)

    except IndexError:
        print("Not enough arguments: four required")
        sys.exit(1)  # abort

    # ensure that the weights add to 100
    if not (tests_df.groupby('course_id', as_index=False)['weight'].sum()['weight'] == 100).all():

        error_dict = {"error": "Invalid course weights"}

        # write error message to json
        with open('output.json', 'w', encoding='utf-8') as outfile:
            json.dump(error_dict, outfile, ensure_ascii=False, indent=2)

    else:
        # merge all the dataframes
        merged_df = marks_df.merge(tests_df, how='left', left_on=['test_id'], right_on=['id']) \
            .merge(students_df, how='left', left_on=['student_id'], right_on=['id']) \
            .merge(courses_df, how='left', left_on=['course_id'], right_on=['id'])

        # add col for weighted average to be summed
        merged_df['courseAverage'] = merged_df['mark'] / 100 * merged_df['weight']

        # compute the average for each class
        courses = merged_df.groupby(['student_id', 'name_x', 'name_y', 'teacher', 'id'], as_index=False)[
            'courseAverage'] \
            .sum().round(2)

        # compute the total average per student
        courses['totalAverage'] = courses['courseAverage'].groupby(courses['student_id']).transform('mean').round(2)

        # rename
        courses.rename(columns={'name_y': 'name'}, inplace=True)

        # create the json formatting
        j = (courses.groupby(['student_id', 'name_x', 'totalAverage'])
             .apply(lambda x: x[['id', 'name', 'teacher', 'courseAverage']].to_dict(orient='records'))
             .reset_index(name='courses')
             .rename(columns={'student_id': 'id', 'name_x': 'name'})
             .to_dict(orient='records'))

        # add the students key
        student_dict = {'students': j}

        # write to file
        with open('output.json', 'w', encoding='utf-8') as outfile:
            json.dump(student_dict, outfile, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main
