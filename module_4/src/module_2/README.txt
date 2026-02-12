## Greg Miller ##
## JHU Software Concepts - Module 2 (Web Scraping) - Feb 1 2026 ##
Approach: 
- Checked robots.txt first to ensure there are no restrictions on scraping from grad cafe. See screenshot. No reference to my client or any client I am using for this assignment restricting access.
- Started by looking at the html of grad cafe to figure out the most efficient/effective way to scrape the data for scrape_data(). Originally started by doing 1 request per data point by using the link:
    https://www.thegradcafe.com/result/XXXXXXXX. Realized these pages did not have all required data, so I decided to scrape from the /survey/ page instead, which also made the script more efficient. 
- Decided to make save_data fairly simple, which just saves a python dict as a json file. This function is used twice, once after scraping the data (which allows me to not have to scrape_data every time I run clean_data)
    and again once the data is cleaned in clean_data. 
- Similarly to save_data, I created load_data to simply load json data and convert to a python dict for easier processing. This is used after the data is scraped to pass to clean_data
- I then began writing the logic for clean_data. This took a lot of work analyzing patterns in all of the different data entries to extract out the appropriate text and account for edge cases. 
    I went through the required data to extract line by line and used combination of string functions, regex and bs4 to extract out the proper data. in a consistent format.
- Once I had my cleaned data, I installed app.py. This was a struggle for me with my setup (running on macbook from 2016). Some of the dependencies for app.py were not compatible with my OS. 
    I had to change the requirements.txt file for app.py to use an older version of llama. I also don't have a very powerful laptop so I had to go back and forth with my AI agent to optimized the app.py code.
    As a result, the code is a fair bit different than the code provided, but it does run pretty fast for how bad my setup is (1 row/1.15 s). Specifically, code was added to optimize the number of threads/cores used for my setup. 
    The calls were also done in batches to parallelize the task. In addition, the prompt was slightly modified to take into account that I had already separated the programs and universities in my parsing. 
    This made the model more efficient and faster. 

To Run: 
Run the requirements.txt
Run clean.py from terminal