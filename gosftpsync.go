package main

import (
	"fmt"
	"io"
	"io/fs"
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

	// Package starts
	start := time.Now()
	iLog.Printf("Starting gosftpsync at %v\n", start)

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

	// list Processed files
	if verbose {
		iLog.Println("VERBOSE: Listing processed sftp files")
	}
	processedSFTPFiles, err := listSFTPFiles(*sc, cfg.SFTPConfig.ReadPath)
	if err != nil {
		iLog.Fatalf("Error listing processed sftp files. Err: %v\n", err)
	}

	if verbose {
		iLog.Printf("VERBOSE: Finished Listing %d processed sftp files", len(processedSFTPFiles))
	}
	archivedSFTPFiles, err := listSFTPFiles(*sc, cfg.SFTPConfig.ArchivedPath)
	if err != nil {
		iLog.Fatalf("Error listing archived sftp files. Err: %s\n", err)
	}

	if verbose {
		iLog.Printf("VERBOSE: Finished Listing %d archived sftp files", len(archivedSFTPFiles))
	}

	if verbose {
		iLog.Println("VERBOSE: Getting diff files")
	}
	filesToDownload := getDiffFileNames(processedSFTPFiles, archivedSFTPFiles)
	iLog.Printf("Found %v new files. Downloading\n", len(filesToDownload))
	if verbose {
		iLog.Println("VERBOSE: starting to download files")
	}
	err = downloadFiles(*sc, filesToDownload, cfg.SFTPConfig.ReadPath, cfg.SFTPConfig.ArchivedPath, cfg.SFTPConfig.DownloadPath)

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
func getDiffFileNames(processedSFTPFiles, archivedSFTPFiles []fs.FileInfo) []string {
	archived := make(map[string]struct{}, len(archivedSFTPFiles))
	for _, af := range archivedSFTPFiles {
		archived[af.Name()] = struct{}{}
	}
	var diff []string
	for _, nf := range processedSFTPFiles {
		if _, found := archived[nf.Name()]; !found {
			diff = append(diff, nf.Name())
		}
	}
	if verbose {
		iLog.Println("VERBOSE: finished getting diff files")
	}

	return diff
}

func downloadFiles(sc sftp.Client, files []string, remoteReadPath, remoteArchivePath, downloadPath string) (err error) {
	for i, fileName := range files {
		iLog.Printf("Working on file %d of %d", i+1, len(files))
		err = downloadRemoteFile(sc, fmt.Sprintf("%s/%s", remoteReadPath, fileName), fmt.Sprintf("%s/%s", downloadPath, fileName))
		if err != nil {
			return err
		}
		err = archiveRemoteFile(sc, fmt.Sprintf("%s/%s", remoteArchivePath, fileName), fmt.Sprintf("%s/%s", remoteReadPath, fileName))
		if err != nil {
			return err
		}
	}
	return
}

// Download file from sftp server
func downloadRemoteFile(sc sftp.Client, remoteReadFile, localFile string) (err error) {

	//  Open file in sftp server
	srcFile, err := sc.OpenFile(remoteReadFile, (os.O_RDONLY))
	if err != nil {
		return fmt.Errorf("Unable to open remote file: %v\n", err)

	}
	defer srcFile.Close()

	// create local file
	dstLocalFile, err := os.Create(localFile)
	if err != nil {
		return fmt.Errorf("Unable to open local file: %v\n", err)

	}
	defer dstLocalFile.Close()

	// copy file from sftp to localfile
	_, err = io.Copy(dstLocalFile, srcFile)
	if err != nil {
		return fmt.Errorf("Unable to copy remote file: %v\n", err)
	}

	if verbose {
		iLog.Printf("VERBOSE: finished processing file %v\n", localFile)
	}
	return
}

// Archive file in sftp server
func archiveRemoteFile(sc sftp.Client, remoteArchiveFile, remoteReadName string) (err error) {
	err = sc.Rename(remoteReadName, remoteArchiveFile)
	if err != nil {
		return fmt.Errorf("Unable to move remote file: %v\n", err)
	}
	if verbose {
		iLog.Printf("VERBOSE: finished moving file %v\n", remoteReadName)
	}
	return
}
