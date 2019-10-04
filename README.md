# KV-Storage
Own key-value database in which information is stored in byte-files

Task: 

	Key Value Storage
	
Engineer:

	Kogut Ivan ФИИТ-101 
	
Description:

	Example of executing programme:
				python KV-Storage.py add data.bin Ivan Kogut
	In this version only 26214400 bytes are available for storing data. All data is located in one binary file (which you initialized).
	Data is stored in tree.
  	12 commands can be used by user:
					Initialize data file
					Add pair "Key-Value"
					Add pair "Key-Content of file"
					Write value by the key (if value is string or integer)
					Copying value by the key to other file (not obligatory to data file) 
					Find out if pair "Key-Value" is contained in data file
					Erase value by the key
					Clear data file
					Change value by the key (if it exists)
					Check if the file is data file
					Add big package of data
					Write all the keys in data file
  	Key can be positive integer or string.
  	Value can be string or file
	For command add_package format of csv file must be like this:
								data,{key},{value}
								file,{key},{path_to_file}


usage: KV-Storage.py [-h]
                     {add,add_file,get,get_file,contains,erase,init,clear,change,check_validity_of_file,cvf,add_package,get_all_keys}
                     ...
		   
To use KV-Storage write one of the positional arguments

	positional arguments:

	{add,add_file,get,get_file,contains,erase,init,clear,change,check_validity_of_file,cvf,add_package,get_all_keys}
	add                 Command to add element(not file) in KV-Storage
	add_file            Command to add file in KV-Storage
	get                 Command to get value(not file) by key
	get_file            Command to get the content of file in KV-Storage. This
			    content goes to your specified file. If file doesn't
			    exist, programme will create it
	contains            Command to find out if element with such key is in KV-
			    Storage or not
	erase               Command to erase element with such key from KV-Storage
	init                Command to create new KV-Storage file
	clear               Command to clear the content of data file
	change              Command to change value of the element with such key
	check_validity_of_file (cvf)
			Command to check if specified file is a KV-Storage
			file(data file)
	add_package         Command to add package of items to KV-Storage
	get_all_keys        Command to get list of all keys in KV-Storage

	optional arguments:
	  -h, --help            show this help message and exit



usage: KV-Storage.py add [-h] file key value

Command to add element(not file) in KV-Storage

	positional arguments:
	  file        data file you want to work with
	  key         key of the element you want to add
	  value       value of the element you want to add

	optional arguments:
	  -h, --help  show this help message and exit


usage: KV-Storage.py add_file [-h] file key path_to_file

Command to add file in KV-Storage

	positional arguments:
	  file          data file you want to work with
	  key           key of the element you want to add
	  path_to_file  path to file which you want to add

	optional arguments:
	  -h, --help    show this help message and exit


usage: KV-Storage.py get [-h] file key

Command to get value(not file) by key

	positional arguments:
	  file        data file you want to work with
	  key         key of the value you want to get

	optional arguments:
	  -h, --help  show this help message and exit


usage: KV-Storage.py get_file [-h] data_file key path_to_output_file

Command to get the content of file in KV-Storage. This content goes to your
specified file. If file doesn't exist, programme will create it

	positional arguments:
	  data_file            data file you want to work with
	  key                  key of the element you want to get
	  path_to_output_file  path to file in which content will store

	optional arguments:
	  -h, --help           show this help message and exit


usage: KV-Storage.py contains [-h] file key

Command to find out if element with such key is in KV-Storage or not

	positional arguments:
	  file        data file you want to work with
	  key         key which existence interests you

	optional arguments:
	  -h, --help  show this help message and exit


usage: KV-Storage.py erase [-h] file key

Command to erase element with such key from KV-Storage

	positional arguments:
	  file        data file you want to work with
	  key         key of the element you want to erase

	optional arguments:
	  -h, --help  show this help message and exit


usage: KV-Storage.py clear [-h] file

Command to clear the content of data file

	positional arguments:
	  file        data file which you want to clear

	optional arguments:
	  -h, --help  show this help message and exit


usage: KV-Storage.py change [-h] file key {file,data} value

Command to change value of the element with such key

	positional arguments:
	  file         data file you want to work with
	  key          key of the element you want to change
	  {file,data}  type of new value(file or data)
	  value        new value

	optional arguments:
	  -h, --help   show this help message and exit


usage: KV-Storage.py check_validity_of_file [-h] file (KV-Storage.py cvf [-h] file)

Command to check if specified file is a KV-Storage file(data file)

	positional arguments:
	  file        file which you want to inspect

	optional arguments:
	  -h, --help  show this help message and exit


usage: KV-Storage.py add_package [-h] [-f [CSV_FILE]] data_file

Command to add package of items to KV-Storage

	positional arguments:
	  data_file      data file you want to work with

	optional arguments:
	  -h, --help     show this help message and exit
	  -f [CSV_FILE]  if you want to read queries from csv file


usage: KV-Storage.py get_all_keys [-h] data_file

Command to get list of all keys in KV-Storage

	positional arguments:
	  data_file   data file you want to work with

	optional arguments:
	  -h, --help  show this help message and exit


usage: KV-Storage.py init [-h] data_file

Command to create new KV-Storage file

	positional arguments:
	  data_file   path to file you want to create

	optional arguments:
	  -h, --help  show this help message and exit
