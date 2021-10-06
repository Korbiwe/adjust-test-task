package main

import (
	"archive/tar"
	"compress/gzip"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

const (
	defaultTarLink  = "https://github.com/adjust/analytics-software-engineer-assignment/blob/master/data.tar.gz?raw=true"
	tempDirName     = "adjustTestTaskData"
	actorsFilename  = "actors.csv"
	commitsFilename = "commits.csv"
	eventsFilename  = "events.csv"
	reposFilename   = "repos.csv"
)

var (
	ErrCantDownloadArchive          = errors.New("couldn't download the archive")
	ErrUnhandledHeaderTypeInArchive = errors.New("unhandled header type inside the tar archive")
	ErrInvalidEventCSV              = errors.New("invalid event csv")
	ErrInvalidUserCSV               = errors.New("invalid user csv")
	ErrInvalidCommitCSV             = errors.New("invalid commit csv")
	ErrInvalidRepoCSV               = errors.New("invalid commit csv")
)

type Event struct {
	ID      string
	Type    string
	ActorID string
	RepoID  string
}

func NewEventFromCSV(csv []string) (*Event, error) {
	if len(csv) != 4 {
		return nil, ErrInvalidEventCSV
	}
	return &Event{
		ID:      csv[0],
		Type:    csv[1],
		ActorID: csv[2],
		RepoID:  csv[3],
	}, nil
}

type User struct {
	ID       string
	Username string
}

func NewUserFromCSV(csv []string) (*User, error) {
	if len(csv) != 2 {
		return nil, ErrInvalidUserCSV
	}
	return &User{
		ID:       csv[0],
		Username: csv[1],
	}, nil
}

type Commit struct {
	Hash    string
	Message string
	EventID string
}

func NewCommitFromCSV(csv []string) (*Commit, error) {
	if len(csv) != 3 {
		return nil, ErrInvalidCommitCSV
	}
	return &Commit{
		Hash:    csv[0],
		Message: csv[1],
		EventID: csv[2],
	}, nil
}

type Repo struct {
	ID   string
	Name string
}

func NewRepoFromCSV(csv []string) (*Repo, error) {
	if len(csv) != 2 {
		return nil, ErrInvalidRepoCSV
	}
	return &Repo{
		ID:   csv[0],
		Name: csv[1],
	}, nil
}

type CommitRatableRepo struct {
	ID      string
	Name    string
	Commits int64
}

func (r *CommitRatableRepo) GetRating() float64 {
	return float64(r.Commits)
}

func (r *CommitRatableRepo) Pretty() string {
	return fmt.Sprintf("ID: %s; Name: %s; Commits: %d;", r.ID, r.Name, r.Commits)
}

type WatchRatableRepo struct {
	ID          string
	Name        string
	WatchEvents int64
}

func (r *WatchRatableRepo) GetRating() float64 {
	return float64(r.WatchEvents)
}

func (r *WatchRatableRepo) Pretty() string {
	return fmt.Sprintf("ID: %s; Name: %s; WatchEvents: %d;", r.ID, r.Name, r.WatchEvents)
}

type RatableUser struct {
	ID       string
	Username string
	Commits  int64
	PREvents int64
}

func (u *RatableUser) GetRating() float64 {
	return float64(u.PREvents + u.Commits)
}

func (u *RatableUser) Pretty() string {
	return fmt.Sprintf("ID: %s; Username: %s; Commits: %d; PREvents: %d;", u.ID, u.Username, u.Commits, u.PREvents)
}

type RepoWithCommitsAndWatches struct {
	ID          string
	Name        string
	Commits     int64
	WatchEvents int64
}

type App struct {
	tempDir string
}

func extractTar(gzipStream io.Reader, path string) error {
	// mitigating a path traversal vulnerability falls out of the test task scope.
	// let's assume we trust the data, as nothing contrary to this was specified in the task description.
	uncompressed, err := gzip.NewReader(gzipStream)
	if err != nil {
		return err
	}

	tarReader := tar.NewReader(uncompressed)
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.Mkdir(filepath.Join(path, header.Name), 0755); err != nil {
				return err
			}
		case tar.TypeReg:
			// wrapping it with a func here to take advantage of defer being called as early as possible
			// could be extracted into a named function for better readability, but im kinda lazy rn
			err := func() error {
				out, err := os.Create(filepath.Join(path, header.Name))
				if err != nil {
					return err
				}
				defer func() {
					_ = out.Close()
					// TODO: log error
				}()
				if _, err := io.Copy(out, tarReader); err != nil {
					return err
				}
				return nil
			}()
			if err != nil {
				return err
			}
		default:
			return ErrUnhandledHeaderTypeInArchive
		}
	}

	return nil
}

func (a *App) DownloadArchive(url string) error {
	resp, err := http.Get(url)
	defer func() {
		_ = resp.Body.Close()
		// TODO: log err
	}()
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return ErrCantDownloadArchive
	}

	tempDir, err := ioutil.TempDir("", fmt.Sprintf("%s*", a.tempDir))
	if err != nil {
		return err
	}
	a.tempDir = tempDir

	if err = extractTar(resp.Body, a.tempDir); err != nil {
		return err
	}

	return nil
}

func (a *App) countCommitsByEvent(eventID string) (int64, error) {
	total := int64(0)
	commitsFile, err := os.Open(filepath.Join(a.tempDir, "data", commitsFilename))
	defer func() {
		_ = commitsFile.Close()
		// TODO: log errors
	}()
	if err != nil {
		return 0, nil
	}

	commitsReader := csv.NewReader(commitsFile)

	wasHeaderRead := false
	for {
		record, err := commitsReader.Read()

		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, err
		}

		// skip the header
		if !wasHeaderRead {
			wasHeaderRead = true
			continue
		}

		commit, err := NewCommitFromCSV(record)
		if err != nil {
			return 0, err
		}

		if commit.EventID != eventID {
			continue
		}

		total += 1
	}

	return total, nil
}

func (a *App) countUserRating(userID string) (int64, int64, error) {
	totalCommits := int64(0)
	totalPREvents := int64(0)
	eventsFile, err := os.Open(filepath.Join(a.tempDir, "data", eventsFilename))
	defer func() {
		_ = eventsFile.Close()
		// TODO: log errors
	}()
	if err != nil {
		return 0, 0, err
	}

	eventsReader := csv.NewReader(eventsFile)
	wasHeaderRead := false
	for {
		record, err := eventsReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, 0, err
		}

		// skip the header
		if !wasHeaderRead {
			wasHeaderRead = true
			continue
		}

		event, err := NewEventFromCSV(record)
		if err != nil {
			return 0, 0, err
		}

		if event.ActorID != userID {
			continue
		}
		switch event.Type {
		case eventTypePush:
			commitCount, err := a.countCommitsByEvent(event.ID)
			if err != nil {
				return 0, 0, err
			}

			totalCommits += commitCount
		case eventTypePullRequest:
			totalPREvents += 1
		default:
			continue
		}
	}

	return totalCommits, totalPREvents, nil
}

func (a *App) countRepoRating(repoID string) (int64, int64, error) {
	commitsPushed := int64(0)
	watchEvents := int64(0)

	eventsFile, err := os.Open(filepath.Join(a.tempDir, "data", eventsFilename))
	defer func() {
		_ = eventsFile.Close()
		// TODO: log errors
	}()
	if err != nil {
		return 0, 0, err
	}

	eventsReader := csv.NewReader(eventsFile)
	wasHeaderRead := false
	for {
		record, err := eventsReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, 0, err
		}

		// skip the header
		if !wasHeaderRead {
			wasHeaderRead = true
			continue
		}

		event, err := NewEventFromCSV(record)
		if err != nil {
			return 0, 0, err
		}

		if event.RepoID != repoID {
			continue
		}

		switch event.Type {
		case eventTypePush:
			commitsByEvent, err := a.countCommitsByEvent(event.ID)
			if err != nil {
				return 0, 0, err
			}
			commitsPushed += commitsByEvent
		case eventTypeWatch:
			watchEvents += 1
		default:
			continue
		}
	}

	return commitsPushed, watchEvents, nil
}

func (a *App) rateReposByCommitsAndWatchesSpaceOptimized() (*Rating, *Rating, error) {
	// first rating is by commits pushed, second is by watch events
	commitsRating := NewRating(10)
	watchRating := NewRating(10)

	reposFile, err := os.Open(filepath.Join(a.tempDir, "data", reposFilename))
	defer func() {
		_ = reposFile.Close()
		// TODO: log error
	}()
	if err != nil {
		return nil, nil, err
	}

	reposReader := csv.NewReader(reposFile)

	wasHeaderRead := false
	for {
		record, err := reposReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, err
		}
		if !wasHeaderRead {
			wasHeaderRead = true
			continue
		}

		repo, err := NewRepoFromCSV(record)
		if err != nil {
			return nil, nil, err
		}

		fmt.Printf("Rating repo %s (ID: %s)... \n", repo.Name, repo.ID)

		commitsCount, watchCount, err := a.countRepoRating(repo.ID)
		if err != nil {
			return nil, nil, err
		}

		fmt.Printf("Repo %s (ID: %s) rated; Commits: %d; Watches: %d; \n\n", repo.Name, repo.ID, commitsCount, watchCount)

		commitsRating.TryPush(&CommitRatableRepo{ID: repo.ID, Name: repo.Name, Commits: commitsCount})
		watchRating.TryPush(&WatchRatableRepo{ID: repo.ID, Name: repo.Name, WatchEvents: watchCount})
	}

	return commitsRating, watchRating, nil
}

func (a *App) rateUsersByPRsAndCommitsSpaceOptimized() (*Rating, error) {
	rating := NewRating(10)
	usersFile, err := os.Open(filepath.Join(a.tempDir, "data", actorsFilename))
	defer func() {
		_ = usersFile.Close()
		// TODO: log errors
	}()
	if err != nil {
		return nil, err
	}

	usersReader := csv.NewReader(usersFile)

	wasHeaderRead := false
	for {
		record, err := usersReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		// skip the header
		if !wasHeaderRead {
			wasHeaderRead = true
			continue
		}

		user, err := NewUserFromCSV(record)
		if err != nil {
			return nil, err
		}

		fmt.Printf("Rating user %s (ID: %s)...\n", user.Username, user.ID)

		commitCount, prCount, err := a.countUserRating(user.ID)
		if err != nil {
			return nil, err
		}

		fmt.Printf("Finished rating user %s (ID: %s); Commits: %d; PR events: %d; \n\n", user.Username, user.ID, commitCount, prCount)

		rating.TryPush(&RatableUser{ID: user.ID, Username: user.Username, Commits: commitCount, PREvents: prCount})
	}

	return rating, nil
}

func (a *App) SpaceOptimizedRatings() (*Rating, *Rating, *Rating, error) {
	usersRating, err := a.rateUsersByPRsAndCommitsSpaceOptimized()
	if err != nil {
		return nil, nil, nil, err
	}

	repoCommitsRating, repoWatchesRating, err := a.rateReposByCommitsAndWatchesSpaceOptimized()
	if err != nil {
		return nil, nil, nil, err
	}

	return usersRating, repoCommitsRating, repoWatchesRating, nil
}

func (a *App) fillUsernames(users map[string]*RatableUser) error {
	usersFile, err := os.Open(filepath.Join(a.tempDir, "data", actorsFilename))
	defer func() {
		_ = usersFile.Close()
		// TODO: log errors
	}()
	if err != nil {
		return err
	}

	usersReader := csv.NewReader(usersFile)

	wasHeaderRead := false
	for {
		record, err := usersReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		// skip the header
		if !wasHeaderRead {
			wasHeaderRead = true
			continue
		}

		user, err := NewUserFromCSV(record)
		if err != nil {
			return err
		}

		rUser, ok := users[user.ID]
		if ok {
			rUser.Username = user.ID
		}
	}

	return nil
}

func (a *App) fillRepoNames(repos map[string]*RepoWithCommitsAndWatches) error {
	reposFile, err := os.Open(filepath.Join(a.tempDir, "data", reposFilename))
	defer func() {
		_ = reposFile.Close()
		// TODO: log errors
	}()
	if err != nil {
		return err
	}

	reposReader := csv.NewReader(reposFile)

	wasHeaderRead := false
	for {
		record, err := reposReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		// skip the header
		if !wasHeaderRead {
			wasHeaderRead = true
			continue
		}

		repo, err := NewRepoFromCSV(record)
		if err != nil {
			return err
		}

		rRepo, ok := repos[repo.ID]
		if ok {
			rRepo.Name = repo.Name
		}
	}

	return nil
}

func (a *App) PerformanceOptimizedRatings() (*Rating, *Rating, *Rating, error) {
	fmt.Println("Starting a performance optimized rating...")
	users := make(map[string]*RatableUser)
	repos := make(map[string]*RepoWithCommitsAndWatches)

	usersRating := NewRating(10)
	repoCommitsRating := NewRating(10)
	repoWatchesRating := NewRating(10)

	eventsFile, err := os.Open(filepath.Join(a.tempDir, "data", eventsFilename))
	defer func() {
		_ = eventsFile.Close()
		// TODO: log errors
	}()
	if err != nil {
		return nil, nil, nil, err
	}

	eventsReader := csv.NewReader(eventsFile)
	wasHeaderRead := false
	for {
		record, err := eventsReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, nil, err
		}

		// skip the header
		if !wasHeaderRead {
			wasHeaderRead = true
			continue
		}

		event, err := NewEventFromCSV(record)
		fmt.Printf("Processing event %s...\n", event.ID)

		switch event.Type {
		case eventTypePush:
			commitsCount, err := a.countCommitsByEvent(event.ID)
			if err != nil {
				return nil, nil, nil, err
			}
			user, ok := users[event.ActorID]
			if !ok {
				users[event.ActorID] = &RatableUser{ID: event.ActorID, Commits: commitsCount}
			} else {
				user.Commits += commitsCount
			}

			repo, ok := repos[event.RepoID]
			if !ok {
				repos[event.RepoID] = &RepoWithCommitsAndWatches{ID: event.RepoID, Commits: commitsCount}
			} else {
				repo.Commits += commitsCount
			}
		case eventTypePullRequest:
			user, ok := users[event.ActorID]
			if !ok {
				users[event.ActorID] = &RatableUser{ID: event.ActorID, PREvents: 1}
			} else {
				user.PREvents += 1
			}
		case eventTypeWatch:
			repo, ok := repos[event.RepoID]
			if !ok {
				repos[event.RepoID] = &RepoWithCommitsAndWatches{ID: event.RepoID, WatchEvents: 1}
			} else {
				repo.WatchEvents += 1
			}
		default:
			continue
		}

		fmt.Printf("Event %s processed.\n\n", event.ID)
	}

	err = a.fillUsernames(users)
	if err != nil {
		return nil, nil, nil, err
	}
	err = a.fillRepoNames(repos)
	if err != nil {
		return nil, nil, nil, err
	}

	for _, value := range users {
		usersRating.TryPush(value)
	}

	for _, value := range repos {
		repoCommitsRating.TryPush(&CommitRatableRepo{ID: value.ID, Name: value.Name, Commits: value.Commits})
		repoWatchesRating.TryPush(&WatchRatableRepo{ID: value.ID, Name: value.Name, WatchEvents: value.WatchEvents})
	}

	return usersRating, repoCommitsRating, repoWatchesRating, nil
}

func (a *App) Cleanup() error {
	if a.tempDir != "" {
		if err := os.RemoveAll(a.tempDir); err != nil {
			return err
		}
	}
	return nil
}

func main() {
	tarLink := flag.String("tarLink", defaultTarLink, "tar link to download the data from")
	flag.Parse()
	app := &App{tempDir: tempDirName}

	if err := app.DownloadArchive(*tarLink); err != nil {
		panic(err)
	}
	defer func() {
		_ = app.Cleanup()
		// TODO: log error
	}()

	poStartTime := time.Now()
	usersRatingPO, repoCommitsRatingPO, repoWatchesRatingPO, err := app.PerformanceOptimizedRatings()
	if err != nil {
		panic(err)
	}
	poTimeTaken := time.Since(poStartTime)

	fmt.Printf("\nRatings: \n")
	fmt.Printf("Users: \n%s \n", usersRatingPO.Pretty())
	fmt.Printf("Repo commits: \n%s \n", repoCommitsRatingPO.Pretty())
	fmt.Printf("Repo watches: \n%s \n", repoWatchesRatingPO.Pretty())

	fmt.Printf("\nTimings: \n")
	fmt.Printf("PO: %s \n", poTimeTaken)
}
