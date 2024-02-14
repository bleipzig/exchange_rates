Pull historical exchange rates, given currency codes. 

- API docs: https://docs.abstractapi.com/exchange-rates
- Endpoint:   https://exchange-rates.abstractapi.com/v1/historical

The API allows users to pull daily information. This helper uses a generator 
function to iterate through a set of dates and concatenate the historical
data to a CSV file.

Run the file using:
```bash
python main.py -s [START DATE] -e [END DATE] -t [TARGETS]
```
where the start and dates are strings, formatted as 'YYYY-MM-DD', and `targets` 
is a string of currency codes, separated by a `,` comma.