log_messages = {
    'manipulate' : 'Manipulating data {}...\n',
    'fetch_data' : 'Fetching data from {}...\n',
    'log' : 'Logging {}...\n',
    'merge' : 'Merging {}...\n',
    'drop' : 'Dropping Duplicates {}...\n',
    'write' : 'Writting file {} to disk...\n',
    'upload' : '{} Uploaded successfully...\n'
}

def print_message(message, attribute=''):
    print(log_messages[message].format(attribute))