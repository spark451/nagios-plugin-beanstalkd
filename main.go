package main

/*
This Plugin takes checks the beanstalk queues at host and compares them to warning and critical values raising warnings
if these thresholds are violated. They can be set via command line opts for all queues or for specific queues using the
following syntax
queue-name=100,500 would set warn to 100 and crit to 500 for queue-name. This must come after all other command line opts

*/


import (
	"fmt"
	"github.com/iwanbk/gobeanstalk"
	"gopkg.in/yaml.v2"
	"strings"
	"log"
	"flag"
	"os"
	"strconv"
)

type Tubes struct {
	Name                string `yaml:"name"`
	CurrentJobsUrgent   int    `yaml:"current-jobs-urgent"`
	CurrentJobsReady    int    `yaml:"current-jobs-ready"`
	CurrentJobsReserved int    `yaml:"current-jobs-reserved"`
	CurrentJobsDelayed  int    `yaml:"current-jobs-delayed"`
	CurrentJobsBuried   int    `yaml:"current-jobs-buried"`
	TotalJobs           int    `yaml:"total-jobs"`
	CurrentUsing        int    `yaml:"current-using"`
	CurrentWatching     int    `yaml:"current-watching"`
	CmdDelete           int    `yaml:"cmd-delete"`
	CmdPauseTube        int    `yaml:"cmd-pause-tube"`
	Pause               int    `yaml:"pause"`
	PauseTimeLeft       int    `yaml:"pause-time-left"`
}

func main() {
	status, err := checkQueues()

	if err!= nil{
		log.Fatal(err)
	}

	os.Exit(status)

}

func checkQueues()(status int, err error){

	hostPtr := flag.String("host", "127.0.0.1", "Host or IP of BeanstalkdServer")
	portPtr := flag.String("port", "11300", "Port of BS Server")
	warnPtr := flag.Int("warn", 100, "This many queued Jobs is  warning")
	critPtr := flag.Int("crit", 1000, "This many queued jobs is critical")

	flag.Parse()

	crit := make(map[string]int)
	warn := make(map[string]int)


	for _, arg := range flag.Args(){
		tubeName := strings.Split(arg,"=")
		values := strings.Split(tubeName[1], ",")

		warn[tubeName[0]], err = strconv.Atoi(values[0])
		if err != nil{
			log.Fatal(err)
		}

		crit[tubeName[0]], err = strconv.Atoi(values[1])
		if err != nil{
			log.Fatal(err)
		}
	}


	if *critPtr <= *warnPtr{
		log.Fatal("Crit must be larger than warn value")
	}

	conn, err := gobeanstalk.Dial(*hostPtr + ":" + *portPtr)
	if err != nil {
		log.Print(err, " Could not connect")
		os.Exit(3)
	}

	defer conn.Quit()


	stats, err := conn.ListTubes()
	if err != nil {
		log.Fatal(err)
	}
	lines := string(stats)

	tubes := strings.Split(lines, "\n")

	status = 0

	for _, tube := range tubes {
		var tubeStats Tubes
		tube = strings.TrimPrefix(tube, "- ")
		if len(tube) < 4 {
			continue
		}

		if _, ok := crit[tube]; !ok {
			crit[tube], warn[tube] = *critPtr, *warnPtr
		}

		stats, err := conn.StatsTube(tube)
		if err != nil {
			log.Fatal(err, string(tube))
		}

		err = yaml.Unmarshal(stats, &tubeStats)
		if err != nil {
			log.Fatal(err, string(stats))
		}

		switch {
		case tubeStats.CurrentJobsReady < warn[tube]:
			fmt.Print(" OK ")

		case tubeStats.CurrentJobsReady < crit[tube]:
			fmt.Print(" warning ")
			if status < 2{
				status = 1
			}


		case tubeStats.CurrentJobsReady >= crit[tube]:
			fmt.Print(" Critical ")
			status = 2

		}

		fmt.Print(tubeStats.Name + " Jobs Ready:", tubeStats.CurrentJobsReady, ";;  ")

	}

	return
}