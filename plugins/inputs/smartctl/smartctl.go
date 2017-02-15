// Package smartctl is a collector for S.M.A.R.T data for HDD, SSD + NVMe devices, linux only
// https://www.smartmontools.org/
package smartctl

import (
	"bytes"
	"fmt"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/inputs"
)

// disk is the struct to capture the specifics of a given device
// It will store some basic information plus anything extended queries return
type disk struct {
	name      string
	vendor    string
	product   string
	block     string
	serial    string
	rotation  string
	transport string
	health    string
	readCache string
	writeback string
	rawData   bytes.Buffer
	stats     diskStats
}

// diskFail is the struct to return from our goroutine parseDisks
type diskFail struct {
	name string
	err  error
}

// diskStats is the set of fields we'll be collecting from smartctl
type diskStats struct {
	currentTemp float64
	maxTemp     float64
	readError   []float64
	writeError  []float64
	verifyError []float64
}

// SmartCtl is the struct that stores our disk paths for checking
type SmartCtl struct {
	Path       string
	Include    []string
	Exclude    []string
	disks      []string
	diskOutput map[string]disk
	diskFailed map[string]error
}

// tagFinders is a global map of Disk struct elements to corresponding regexp
var tagFinders = map[string]*regexp.Regexp{
	"Vendor":    regexp.MustCompile(`Vendor:\s+(\w+)`),
	"Product":   regexp.MustCompile(`Product:\s+(\w+)`),
	"Block":     regexp.MustCompile(`Logical block size:\s+(\w+)`),
	"Serial":    regexp.MustCompile(`Serial number:\s+(\w+)`),
	"Rotation":  regexp.MustCompile(`Rotation Rate:\s+(\w+)`),
	"Transport": regexp.MustCompile(`Transport protocol:\s+(\w+)`),
	"Health":    regexp.MustCompile(`SMART Health Status:\s+(\w+)`),
	"ReadCache": regexp.MustCompile(`Read Cache is:\s+(\w+)`),
	"Writeback": regexp.MustCompile(`Writeback Cache is:\s+(\w+)`),
}

// fieldFinders is a global map of DiskStats struct elements
var fieldFinders = map[string]*regexp.Regexp{
	"CurrentTemp": regexp.MustCompile(`Current Drive Temperature:\s+([0-9]+)`),
	"MaxTemp":     regexp.MustCompile(`Drive Trip Temperature:\s+([0-9]+)`),
}

// sliceFinders is a global map of slices in the DiskStats struct
var sliceFinders = map[string]*regexp.Regexp{
	"ReadError":   regexp.MustCompile(`read:\s+(.*)\n`),
	"WriteError":  regexp.MustCompile(`write:\s+(.*)\n`),
	"VerifyError": regexp.MustCompile(`verify:\s+(.*)\n`),
}

var sampleConfig = `
  ## smartctl requires installation of the smartmontools.
  ##
  # path = "/usr/local/bin/smartctl"
  ##
  ## Users have the ability to specify an list of disk name to include, to exclude,
  ## or both. In this iteration of the collectors, you must specify the full smartctl
  ## path for the disk, we are not currently supporting regex. For example, to include/exclude
  ## /dev/sda from your list, you would specify:
  ## include = ["/dev/sda -d scsi"]
  ## exclude = ['/dev/sda -d scsi"]
  ##
  ## NOTE: If you specify an include list, this will skip the smartctl --scan function
  ## and only collect for those you've requested (minus any exclusions).
  include = ["/dev/bus/0 -d megaraid,24"]
  exclude = ["/dev/sda -d scsi"]
`

// SampleConfig returns the preformatted string on how to use the smartctl collector
func (s *SmartCtl) SampleConfig() string {
	return sampleConfig
}

// Description returns a preformatted string outlining the smartctl collector
func (s *SmartCtl) Description() string {
	return "Use the smartctl (smartmontool) to determine HDD, SSD or NVMe physical status"
}

// parseString is a generic function that takes a given regexp and applies to a buf, placing
// output into the dataVar
func (s *SmartCtl) parseString(regexp *regexp.Regexp, buf *bytes.Buffer, dataVar *string) {
	str := regexp.FindStringSubmatch((*buf).String())

	if len(str) > 1 {
		*dataVar = str[1]
		return
	}

	*dataVar = "none"
}

// ParseStringSlice is a generic function that takes a given regexp and applies to a buf, placing
// output into the dataVar
func (s *SmartCtl) parseStringSlice(regexp *regexp.Regexp, buf *bytes.Buffer, dataVar *[]string) {
	str := regexp.FindStringSubmatch((*buf).String())

	if len(str) > 1 {
		*dataVar = str[1:]
	}
}

// parseFloat is a generic function that takes a given regexp and applies to a buf, placing
// output into the dataVar
func (s *SmartCtl) parseFloat(regexp *regexp.Regexp, buf *bytes.Buffer, dataVar *float64) error {
	str := regexp.FindStringSubmatch((*buf).String())

	if len(str) > 1 {
		var err error
		*dataVar, err = strconv.ParseFloat(str[1], 64)
		if err != nil {
			return fmt.Errorf("[ERROR] Could not convert string (%s) to float64: %v\n", str[1], err)
		}
	}

	return nil
}

// parseFloatSlice is a generic function that takes a given regexp and applies to a buf, placing
// output into the dataVar
func (s *SmartCtl) parseFloatSlice(regexp *regexp.Regexp, buf *bytes.Buffer, dataVar *[]float64) error {
	var errors []string
	var values []float64
	var val float64
	var err error

	str := regexp.FindStringSubmatch((*buf).String())

	if len(str) > 1 {
		for _, each := range strings.Split(str[1], " ") {
			if len(each) <= 0 {
				continue
			} else if val, err = strconv.ParseFloat(each, 64); err != nil {
				errors = append(errors, fmt.Sprintf("[ERROR] Could not parse string (%s) into float64: %v", each, err))
				continue
			}
			values = append(values, val)
		}
	}

	*dataVar = values
	if len(errors) > 0 {
		return fmt.Errorf("%s\n", strings.Join(errors, "\n"))
	}

	return nil
}

// parseDisks is a private function that we call for our goroutine
func (s *SmartCtl) parseDisks(each string, c chan<- disk, e chan<- diskFail) {

	data := disk{name: "empty"}
	var tagMap = map[string]*string{
		"Vendor":    &data.vendor,
		"Product":   &data.product,
		"Block":     &data.block,
		"Serial":    &data.serial,
		"Rotation":  &data.rotation,
		"Transport": &data.transport,
		"Health":    &data.health,
		"ReadCache": &data.readCache,
		"Writeback": &data.writeback,
	}

	var stats diskStats
	var fieldMap = map[string]*float64{
		"CurrentTemp": &stats.currentTemp,
		"MaxTemp":     &stats.maxTemp,
	}

	var sliceMap = map[string]*[]float64{
		"ReadError":   &stats.readError,
		"WriteError":  &stats.writeError,
		"VerifyError": &stats.verifyError,
	}

	disk := strings.Split(each, " ")

	cmd := []string{"-x"}
	cmd = append(cmd, disk...)

	var out []byte
	var err error
	if out, err = exec.Command(s.Path, cmd...).CombinedOutput(); err != nil {
		e <- diskFail{name: each, err: fmt.Errorf("[ERROR] could not collect (%s), err: %v\n", each, err)}
		return
	}

	if _, err := data.rawData.Write(out); err != nil {
		e <- diskFail{name: each, err: fmt.Errorf("[ERROR] could not commit raw data to struct (%s): %v\n", each, err)}
		return
	}

	if len(disk) > 2 {
		data.name = strings.Replace(fmt.Sprintf("%s_%s", disk[0], disk[2]), ",", "_", -1)
	} else {
		data.name = strings.Replace(disk[0], ",", "_", -1)
	}

	// NOTE: for this loop to work you must keep the idx + Disk element names equal
	for idx := range tagFinders {
		s.parseString(tagFinders[idx], &data.rawData, tagMap[idx])
	}

	stats = diskStats{}
	for idx := range fieldFinders {
		if err := s.parseFloat(fieldFinders[idx], &data.rawData, fieldMap[idx]); err != nil {
			fmt.Printf("[ERROR] parseFloat: %v\n", err)
		}
	}

	for idx := range sliceFinders {
		if err := s.parseFloatSlice(sliceFinders[idx], &data.rawData, sliceMap[idx]); err != nil {
			fmt.Printf("[ERROR] parseFloatSlice: %v\n", err)
		}
	}

	data.stats = stats
	c <- data
}

// parseDisks takes in a list of disks and accumulates the smartctl info where possible for each entry
func (s *SmartCtl) parseDisks2() error {
	c := make(chan disk, len(s.disks))
	e := make(chan diskFail, len(s.disks))
	var a int

	for _, each := range s.disks {
		go s.parseDisks(each, c, e)
	}

	for {
		if a == len(s.disks) {
			break
		}

		select {
		case data := <-c:
			if len(data.name) > 0 && data.name != "empty" {
				s.diskOutput[data.name] = data
			}
			a++
		case err := <-e:
			s.diskFailed[err.name] = err.err
			a++
		default:
			time.Sleep(50 * time.Millisecond)
		}
	}

	return nil
}

// splitDisks is a private helper function to parse out the disks we care about
func (s *SmartCtl) splitDisks(out string) []string {
	var disks []string
	for _, each := range strings.Split(out, "\n") {
		if len(each) > 0 {
			disks = append(disks, strings.Split(each, " #")[0])
		}
	}
	return disks
}

func (s *SmartCtl) gatherDisks() error {
	var out []byte
	var err error

	if out, err = exec.Command(s.Path, []string{"--scan"}...).CombinedOutput(); err != nil {
		return fmt.Errorf("[ERROR] Could not gather disks from smartctl --scan: %v\n", err)
	}

	s.disks = s.splitDisks(string(out))
	fmt.Printf("DEBUG: disks: %v\n", strings.Join(s.disks, "|"))
	return nil
}

// excludeDisks is a private function to reduce the set of disks to query against
func (s *SmartCtl) excludeDisks() []string {
	elems := make(map[string]bool)

	for _, each := range s.disks {
		elems[each] = false
	}

	for _, each := range s.Exclude {
		if _, ok := elems[each]; ok {
			delete(elems, each)
		}
	}

	disks := []string{}
	for key := range elems {
		disks = append(disks, key)
	}

	return disks
}

// init adds the smartctl collector as an input to telegraf
func init() {
	m := SmartCtl{}

	path, _ := exec.LookPath("smartctl")
	if len(path) > 0 {
		m.Path = path
	}

	inputs.Add("smartctl", func() telegraf.Input {
		return &m
	})
}

// Gather is the primary function to collect smartctl data
func (s *SmartCtl) Gather(acc telegraf.Accumulator) error {
	var health float64

	// NOTE: if we specify the Include list in the config, this will skip the smartctl --scan
	if len(s.Include) > 0 {
		s.disks = s.splitDisks(strings.Join(s.Include, "\n"))
	} else if err := s.gatherDisks(); err != nil {
		return err
	}

	if len(s.Exclude) > 0 {
		s.disks = s.excludeDisks()
	}

	s.diskOutput = make(map[string]disk, len(s.disks))
	s.diskFailed = make(map[string]error)

	// actually gather the stats
	if err := s.parseDisks2(); err != nil {
		return fmt.Errorf("could not parse all the disks in our list: %v\n", err)
	}

	for _, each := range s.diskOutput {
		tags := map[string]string{
			"name":       each.name,
			"vendor":     each.vendor,
			"product":    each.product,
			"block_size": each.block,
			"serial":     each.serial,
			"rpm":        each.rotation,
			"transport":  each.transport,
			"read_cache": each.readCache,
			"writeback":  each.writeback,
		}

		if each.health == "OK" {
			health = 1.0
		} else {
			health = 0.0
		}

		fields := make(map[string]interface{})
		fields["health"] = health
		fields["current_temp"] = each.stats.currentTemp
		fields["max_temp"] = each.stats.maxTemp

		// add the read error row
		if len(each.stats.readError) == 7 {
			fields["ecc_corr_fast_read"] = each.stats.readError[0]
			fields["ecc_corr_delay_read"] = each.stats.readError[1]
			fields["ecc_reread"] = each.stats.readError[2]
			fields["total_err_corr_read"] = each.stats.readError[3]
			fields["corr_algo_read"] = each.stats.readError[4]
			fields["data_read"] = each.stats.readError[5]
			fields["uncorr_err_read"] = each.stats.readError[6]
		}

		// add the write error row
		if len(each.stats.writeError) == 7 {
			fields["ecc_corr_fast_write"] = each.stats.writeError[0]
			fields["ecc_corr_delay_write"] = each.stats.writeError[1]
			fields["ecc_rewrite"] = each.stats.writeError[2]
			fields["total_err_corr_write"] = each.stats.writeError[3]
			fields["corr_algo_write"] = each.stats.writeError[4]
			fields["data_write"] = each.stats.writeError[5]
			fields["uncorr_err_write"] = each.stats.writeError[6]
		}

		// add the verify error row
		if len(each.stats.verifyError) == 7 {
			fields["ecc_corr_fast_verify"] = each.stats.verifyError[0]
			fields["ecc_corr_delay_verify"] = each.stats.verifyError[1]
			fields["ecc_reverify"] = each.stats.verifyError[2]
			fields["total_err_corr_verify"] = each.stats.verifyError[3]
			fields["corr_algo_verify"] = each.stats.verifyError[4]
			fields["data_verify"] = each.stats.verifyError[5]
			fields["uncorr_err_verify"] = each.stats.verifyError[6]
		}

		acc.AddFields("smartctl", fields, tags)
	}

	return nil
}
