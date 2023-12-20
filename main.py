import csv
import os
import sys

import pandas as pd
from config import files
from dbfread import DBF

application_path = os.path.dirname(sys.executable)


def main():
    input_file = sys.argv[1] if len(sys.argv) > 1 else files['INPUT']
    output_file = sys.argv[2] if len(sys.argv) > 2 else files['OUTPUT']

    curr_path = os.getcwd()
    if input_file is not None:
        if not os.path.exists(input_file):
            input_file_edited = "files/" + input_file
            if not os.path.exists(input_file_edited):
                print(f"{input_file} cannot be found. {input_file} may not exist or path may be incorrect.")
                sys.exit(1)
            else:
                input_file = "files/" + input_file

    input_ = []
    input_columns = []
    input_keys = []
    input_dict = {}

    # clear out the flags file
    with open(files['FLAGS'], 'w') as file:
        pass

    config = []  # 800xA Property, FilesMod, FilesMod Property, Rule, Identifier

    with open(input_file, 'r') as file:
        reader = csv.reader(file)

        for column in next(reader):
            input_columns.append(column)

        i = 0
        for col in input_columns:
            input_dict[col.lower()] = i
            input_keys.append(col.lower())
            i += 1

        for row in reader:
            if row[input_dict['description']] == '':
                input_.append(row)

    with open(files['CONFIG'], 'r') as file:
        reader = csv.reader(file)

        for row in reader:
            config.append(row)

    input_df = pd.read_csv(input_file)

    config_df = pd.read_csv(files['CONFIG'])

    xA_Properties = []
    for index, row in config_df.iterrows():
        xA_Properties.append(row['800xA Property'])

    for prop in xA_Properties:
        if prop not in input_columns:
            add_to_flags_file(f'800xA Property "{prop}" is not found within input file')

    rules_df = pd.read_csv(files['RULES'])

    # Check if the input obj already has a description. If so, remove it from table.
    for i in range(len(input_df)):
        if not pd.isna(input_df.loc[i, 'Description']):
            input_df.drop(i, inplace=True)

    # start
    for config_row_iterator, xA_prop in enumerate(config_df['800xA Property']):
        for op_prop in input_df.head(1):
            if xA_prop == op_prop:
                file_name = config_df.loc[config_row_iterator, 'FilesMod']
                identifier, identifier_row, identifier_rules = get_identifier(file_name)
                file_path = files[file_name]
                records = DBF(file_path, load=True)
                file_df = pd.DataFrame(records)
                mod_property = config_df.loc[config_row_iterator, 'Mod Property']
                # loop through file_df, find where Identifier == Property (Name)
                unique_names = []
                flagged_names = []
                for file_row_iterator, file_row_name in enumerate(file_df[identifier]):
                    # Check if a double exists within a file.
                    # Two lists are used so that multiple copies are only flagged once.
                    if file_row_name in unique_names:
                        if file_row_name not in flagged_names:
                            flag = f"{file_row_name} already exists within {file_name}"
                            add_to_flags_file(flag)
                            flagged_names.append(file_row_name)
                        continue
                    unique_names.append(file_row_name)
                    for name_iterator, name in enumerate(input_df['Name']):
                        original_rules = identifier_rules
                        # edit the file_row_name based on its rules
                        while len(identifier_rules) != 0:
                            rule = rules_df.loc[int(float(identifier_rules[0])) - 1, 'Rule']
                            identifier_rules.pop(0)
                            file_row_name = edit_with_rule(rule, file_row_name, name, rules_df)
                        identifier_rules = original_rules
                        if file_row_name == name:  # if name in input == name in file
                            value = file_df.loc[file_row_iterator, mod_property]
                            # print("name: ", name, "   mod property: ", mod_property, "   " + "value: ", value, end='')
                            rule_num = config_df.loc[config_row_iterator, 'Rule']
                            if not pd.isna(rule_num):
                                # rules are 0 indexed in dataframe but 1 indexed in config.csv
                                rule_num = str(rule_num).split()
                                while len(rule_num) != 0:
                                    rule = rules_df.loc[int(float(rule_num[0])) - 1, 'Rule']
                                    rule_num.pop(0)
                                    value = edit_with_rule(rule, value, name, rules_df)

                                input_df.loc[name_iterator, xA_prop] = value
                            else:
                                input_df.loc[name_iterator, xA_prop] = value
                            break

    if output_file == input_file or len(sys.argv) > 1 or output_file == "":
        output_file = input_file  # in case of 2nd condition so that output_path is not files['OUTPUT']
        output_file = output_file[:-4]
        output_file += "_edited.csv"
    input_df.to_csv(output_file, index=False)
    print(f"The output file is {output_file}")


def edit_with_rule(rule, value, name, rules_df=None):
    """
    :param rule: the current rule
    :param value: the current value that is being edited
    :param name: the name of the input object
    :param rules_df: rules dataframe, for identifier in find_replacement_in_file()
    :return: the updated value
    """
    rule = rule.split('`')
    if rule[0].lower() == 's':
        rule.pop(0)
        while len(rule) != 0:
            value = value.replace(rule[0], rule[1])
            rule.pop(0)
            rule.pop(0)
    elif rule[0].lower() == 'f':
        if value == rule[1]:
            value = find_replacement_in_file(rule[2], name, rules_df)
    elif rule[0].lower() == 'fm':
        if value != rule[1]:
            return
        for i, s in enumerate(rule):
            if s[:2] == "f:":
                r_file_string = s[2:]
                rule[i] = find_replacement_in_file(r_file_string, name, rules_df)

        operation = ""
        for i in range(2, len(rule)):
            operation += rule[i]
        value = eval(operation)

    return value


def find_replacement_in_file(s, name, rules_df):
    """
    :param s: the string that needs to be replaced
    :param name: the name of the input object
    :param rules_df: rules dataframe
    :return: a replacement for s
    """
    file_replacement = s.split(':')
    file_name = file_replacement[0]
    file_value = file_replacement[1]

    # open the file with the replacement value
    records = DBF(files[file_name], load=True)
    r_file_df = pd.DataFrame(records)
    # find the identifier for the file
    identifier, identifier_row, rule_num = get_identifier(file_name)

    for r_name_row, r_name in enumerate(r_file_df[identifier]):
        while len(rule_num) != 0:
            rule = rules_df.loc[int(float(rule_num[0])) - 1, 'Rule']
            rule_num.pop(0)
            r_name = edit_with_rule(rule, r_name, name, r_name_row)
        if r_name == name:
            return r_file_df.loc[r_name_row, file_value]

    print("Error: identifier not found within rule replacement file")
    add_to_flags_file("Error: identifier not found within rule replacement file")
    exit(1)


def get_identifier(file_name: str) -> list:
    """
    :param file_name: the name of the file where the value is from
    :return: a list of the identifier (based on the file_name),
    """
    identifier_df = pd.read_csv(files['IDENTIFIERS'])
    identifier = None

    identifier_row = 0
    #
    for identifier_row, i_name in enumerate(identifier_df['File_Name']):
        if i_name == file_name:
            identifier = identifier_df.loc[identifier_row, 'Identifier']
            break

    identifier_rules = str(identifier_df.loc[identifier_row, 'Rule']).split()

    return [identifier, identifier_row, identifier_rules]


def add_to_flags_file(flag: str) -> None:
    """
    :param flag: the flag to be appended to the flags file
    :return: None
    """
    with open(files['FLAGS'], 'a') as f:
        f.write(flag + '\n')
        print("added to flags")


if __name__ == "__main__":
    main()
