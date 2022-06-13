## running
### using data - amazon_reviews_us_Watches_v1_00.tsv

``` sudo bash scripts/run_cassandra_network.sh ```
![run_cassasndra](https://user-images.githubusercontent.com/70692373/173310883-58476eb2-083e-4a9c-b2b0-de887efa1f00.png)

``` sudo bash scripts/create_tables.sh ```
![create_tables](https://user-images.githubusercontent.com/70692373/173310914-378e66f7-5468-4047-b32f-36701a559a04.png)

``` sudo bash scripts/write_data.sh # the same as python3 main.py ```
![write_data](https://user-images.githubusercontent.com/70692373/173310932-c350d48b-601d-4ee5-a4e5-0cd912822f12.png)

``` sudo bash scripts/run_docker.sh # analog for python3 app.py ```
![app_run](https://user-images.githubusercontent.com/70692373/173311132-12c979fe-4914-44c6-b346-a89ca42f38c7.png)
.

### requests

all test request are saved in requests.txt
I will show only first 3, because with next requests there were some troubles


![request1](https://user-images.githubusercontent.com/70692373/173311919-64493210-5228-4ee3-968b-aad28bcf4072.png)
![request2](https://user-images.githubusercontent.com/70692373/173311922-ab0429af-bf64-4526-b90e-f1694c4df93e.png)
![request3](https://user-images.githubusercontent.com/70692373/173311924-6ea00882-515a-4287-b67f-8d691075a649.png)

