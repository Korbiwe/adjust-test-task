### Adjust test task

This CLI application parses github data to produce 3 different ratings:

- Users rated by amount of commits pushed + PR events
- Repositories by amount of commits pushed
- Repositories by amount of watch events

Usage:

- Compile the application (`go build main.go rating.go eventTypes.go`)
- Run the produced binary (`./main`)

CLI Flags:

- `-tarLink <link>` A link pointing to a tar archive with the data. Defaults to `https://github.com/adjust/analytics-software-engineer-assignment/blob/master/data.tar.gz?raw=true`