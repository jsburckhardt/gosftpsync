package main

import (
	"fmt"
	"io"
	"io/fs"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"time"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
	"gopkg.in/yaml.v3"
)

type Config struct {
	SFTPConfig struct {
		ArchivedPath           string `yaml:"archivepath"`
		DownloadPath           string `yaml:"downloadpath"`
		ConnectionStringEnvVar string `yaml:"connectionstringenvvar"`
		ReadPath               string `yaml:"readpath"`
		Verbose                bool   `yaml:"verbose"`
	} `yaml:"sftpconfig"`
}

var iLog *log.Logger
var verbose bool

func main() {
	// validate arguments
	if len(os.Args) != 3 {
		log.Fatal("Please provide args -> gosftpsync \"<configfilepath>\" \"<loggilepath\"")
	}
	// setup logger
	LOGFILE := os.Args[2]
	f, err := os.OpenFile(LOGFILE, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer f.Close()

	LstdFlags := log.Ldate | log.Lshortfile
	iLog = log.New(f, "gosftpsync ", LstdFlags)
	iLog.SetFlags(log.Lshortfile | log.LstdFlags)

	var cfg Config
	err = readConfigFile(&cfg, os.Args[1])
	verbose = cfg.SFTPConfig.Verbose

	if err != nil {
		iLog.Fatalf("Failed reading config from %s. Err: %+v\n", os.Args[1], err)
	}

	if verbose {
		iLog.Println("VERBOSE: post read config")
	}

	start := time.Now()
	iLog.Printf("Starting gosftpsync at %s\n", start)

	if verbose {
		iLog.Println("VERBOSE: Loading environment variable")
	}

	rawurl := os.Getenv(cfg.SFTPConfig.ConnectionStringEnvVar)
	if rawurl == "" {
		iLog.Fatalf("Can't find environment variable %s\n", cfg.SFTPConfig.ConnectionStringEnvVar)
	}

	if verbose {
		iLog.Println("VERBOSE: Parsing url")
	}

	parsedURL, err := url.Parse(rawurl)
	if err != nil {
		iLog.Fatalf("Parse Url error %s\n", err)
	}

	// get connection details
	if verbose {
		iLog.Println("VERBOSE: Collecting parsed data")
	}
	user := parsedURL.User.Username()
	password, passwordExists := parsedURL.User.Password()
	host := parsedURL.Host
	port := 22
	if !passwordExists {
		iLog.Fatal("Missing password in SFTPTOGO_URL environment variable")
	}

	// Configuring the ssh client
	if verbose {
		iLog.Println("VERBOSE: Configuring ssh client")
	}
	var auths []ssh.AuthMethod
	auths = append(auths, ssh.Password(password))
	config := ssh.ClientConfig{
		User:            user,
		Auth:            auths,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}
	addr := fmt.Sprintf("%s:%d", host, port)
	conn, err := ssh.Dial("tcp", addr, &config)
	if err != nil {
		iLog.Fatalf("Failed to connecto to [%s]: %v\n", addr, err)
		os.Exit(1)
	}
	defer conn.Close()

	// Create new SFTP client
	if verbose {
		iLog.Println("VERBOSE: Configuring sftp client")
	}
	sc, err := sftp.NewClient(conn)
	if err != nil {
		iLog.Fatalf("Unable to start SFTP subsystem: %v\n", err)
		os.Exit(1)
	}
	defer sc.Close()

	// list files
	if verbose {
		iLog.Println("VERBOSE: Listing sftp files")
	}
	SFTPFiles, err := listSFTPFiles(*sc, cfg.SFTPConfig.ReadPath)
	if err != nil {
		iLog.Fatalf("Error listing files: %v\n", err)
	}

	if verbose {
		iLog.Println("VERBOSE: Listing archived files")
	}
	archivedFiles, err := ioutil.ReadDir(cfg.SFTPConfig.ArchivedPath)
	if err != nil {
		iLog.Fatalf("Error listing archived files in path: %s. Err: %s\n", cfg.SFTPConfig.ArchivedPath, err)
	}

	if verbose {
		iLog.Printf("VERBOSE: Finished Listing %d archived files", len(archivedFiles))
	}

	if verbose {
		iLog.Println("VERBOSE: Getting diff files")
	}
	filesToDownload := getDiffFileNames(SFTPFiles, archivedFiles)
	iLog.Printf("Found %v new files. Downloading\n", len(filesToDownload))
	if verbose {
		iLog.Println("VERBOSE: starting to download files")
	}
	err = downloadFiles(*sc, filesToDownload, cfg.SFTPConfig.ReadPath, cfg.SFTPConfig.DownloadPath)

	if err != nil {
		iLog.Fatalf("Failed downloading files. Err: %s\n", err)
	}
	if verbose {
		iLog.Println("VERBOSE: finished download")
	}
	duration := time.Since(start)
	iLog.Printf("Successfully downlaoded %v files. Took %s\n", len(filesToDownload), duration)
}

func readConfigFile(config *Config, configPath string) error {
	if verbose {
		iLog.Println("VERBOSE: reading config file")
	}
	f, err := os.Open(configPath)
	if err != nil {
		return err
	}
	defer f.Close()
	decoder := yaml.NewDecoder(f)
	err = decoder.Decode(&config)
	if err != nil {
		return err
	}
	if verbose {
		iLog.Println("VERBOSE: finished reading config file")
	}
	return nil
}

// list files in the SFTP folder
func listSFTPFiles(sc sftp.Client, remoteDir string) (SFTPList []fs.FileInfo, err error) {
	files, err := sc.ReadDir(remoteDir)
	if err != nil {
		return nil, err
	}
	if verbose {
		iLog.Printf("VERBOSE: finished listing %d sftp files", len(files))
	}
	// ignoring directories
	for i, file := range files {
		if file.IsDir() {
			iLog.Printf("VERBOSE: Removing directory from list %s\n", file.Name())
			files = remove(files, i)
		}
	}
	return files, nil
}

func remove(files []fs.FileInfo, i int) []fs.FileInfo {
	return append(files[:i], files[i+1:]...)
}

// compare files in two directories
func getDiffFileNames(SFTPFiles, archivedFiles []fs.FileInfo) []string {
	archived := make(map[string]struct{}, len(archivedFiles))
	for _, af := range archivedFiles {
		archived[af.Name()] = struct{}{}
	}
	var diff []string
	for _, nf := range SFTPFiles {
		if _, found := archived[nf.Name()]; !found {
			diff = append(diff, nf.Name())
		}
	}
	if verbose {
		iLog.Println("VERBOSE: finished getting diff files")
	}

	return diff
}

func downloadFiles(sc sftp.Client, files []string, readPath, downloadPath string) (err error) {
	for _, name := range files {
		err = downloadFile(sc, fmt.Sprintf("%s/%s", readPath, name), fmt.Sprintf("%s/%s", downloadPath, name))
		if err != nil {
			return err
		}
	}
	return nil
}

// Download file from sftp server
func downloadFile(sc sftp.Client, remoteFile, localFile string) (err error) {

	srcFile, err := sc.OpenFile(remoteFile, (os.O_RDONLY))
	if err != nil {
		iLog.Fatalf("Unable to open remote file: %v\n", err)
		return err
	}
	defer srcFile.Close()

	dstFile, err := os.Create(localFile)
	if err != nil {
		iLog.Fatalf("Unable to open local file: %v\n", err)
		return err
	}
	defer dstFile.Close()

	_, err = io.Copy(dstFile, srcFile)
	if err != nil {
		iLog.Fatalf("Unable to copy remote file: %v\n", err)
		return err
	}
	if verbose {
		iLog.Println("VERBOSE: finished downloading file")
	}
	return
}
