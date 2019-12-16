package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
)

//var ErrBadFileFormat = fmt.Errorf( "unrecognized file format" )
var ErrBadRecord = fmt.Errorf("bad MARC record encountered")

//var ErrBadRecordId = fmt.Errorf("bad MARC record identifier")
var ErrFileNotOpen = fmt.Errorf("file is not open")

// the RecordLoader interface
type RecordLoader interface {
	Source() string
	Validate() error
	First(bool) (Record, error)
	Next(bool) (Record, error)
	Done()
}

// the Marc record interface
type Record interface {
	Id() (string, error)
	Source() string
	SetSource(string)
	Raw() []byte
}

// this is our loader implementation
type recordLoaderImpl struct {
	DataSource string   // determined from the filename
	File       *os.File // our file handle
	HeaderBuff []byte   // buffer for the record header
}

// this is our record implementation
type recordImpl struct {
	RawBytes []byte // the raw record
	source   string // determined from the filename
	marcId   string // extracted from the record
}

//
// This implementation based on the MARC reader code here:
//
// https://github.com/marc4j/marc4j/blob/master/src/org/marc4j/util/RawRecord.java
// https://github.com/marc4j/marc4j/blob/master/src/org/marc4j/util/RawRecordReader.java
//
// All errors in the implementation are mine :)
//

// the size of the MARC record header
var marcRecordHeaderSize = 5
var marcRecordFieldDirStart = 24
var marcRecordFieldDirEntrySize = 12

// terminator sentinel values
var fieldTerminator = byte(0x1e)
var recordTerminator = byte(0x1d)

// and the factory
func NewRecordLoader(remoteName string, localName string) (RecordLoader, error) {

	file, err := os.Open(localName)
	if err != nil {
		return nil, err
	}

	source := getDataSource(remoteName)
	buf := make([]byte, marcRecordHeaderSize)
	return &recordLoaderImpl{File: file, DataSource: source, HeaderBuff: buf}, nil
}

func getDataSource(name string) string {

	//
	// we have a convention for names which can be used to identify a data source
	// typically is is:
	//    /dir-name/source-name/year/file
	//
	// so if we split the file by file separator and get 4 tokens, we can assume that
	// token number 2 is the data source.
	//
	// in the event that we cannot, just put "unknown"
	//

	source := "unknown"
	tokens := strings.Split(name, "/")
	if len(tokens) == 4 {
		source = tokens[1]
	}
	log.Printf("INFO: data source identified is: %s", source)
	return source
}

// read all the records to ensure the file is valid
func (l *recordLoaderImpl) Validate() error {

	if l.File == nil {
		return ErrFileNotOpen
	}

	// get the first record and error out if bad. An EOF is OK, just means the file is empty
	_, err := l.First(false)
	if err != nil {
		// are we done
		if err == io.EOF {
			log.Printf("WARNING: EOF on first read, looks like an empty file")
			return nil
		} else {
			log.Printf("ERROR: validation failure on record index 0")
			return err
		}
	}

	// used for reporting
	recordIndex := 1

	// read all the records and bail on the first failure except EOF
	for {
		_, err = l.Next(false)

		if err != nil {
			// are we done
			if err == io.EOF {
				break
			} else {
				log.Printf("ERROR: validation failure on record index %d", recordIndex)
				return err
			}
		}
		recordIndex++
	}

	// everything is OK
	return nil
}

func (l *recordLoaderImpl) First(readAhead bool) (Record, error) {

	if l.File == nil {
		return nil, ErrFileNotOpen
	}

	// go to the start of the file and then get the next record
	_, err := l.File.Seek(0, 0)
	if err != nil {
		return nil, err
	}

	return l.Next(readAhead)
}

func (l *recordLoaderImpl) Next(readAhead bool) (Record, error) {

	if l.File == nil {
		return nil, ErrFileNotOpen
	}

	rec, err := l.rawMarcRead()
	if err != nil {
		return nil, err
	}

	id, err := rec.Id()
	if err != nil {
		return nil, err
	}

	//log.Printf( "INFO: marc record id: %s", id )

	//
	// there are times when the following record is really part of the current record. If this is the case, the 2 (or more)
	// records are given the same id. Attempt to handle this here.
	//
	if readAhead == true {

		for {
			// get the current position, assume no error cos we are not moving the file pointer
			currentPos, _ := l.File.Seek(0, 1)

			// get the next record
			nextRec, err := l.rawMarcRead()
			if err != nil {

				// if we error move the file pointer back and return the previously read record without error
				_, _ = l.File.Seek(currentPos, 0)
				return rec, nil
			}

			nextId, err := nextRec.Id()
			if err != nil {
				// if we error move the file pointer back and return the previously read record without error
				_, _ = l.File.Seek(currentPos, 0)
				return rec, nil
			}

			if id != nextId {
				// if the id's do not match move the file pointer back and return the previously read record
				_, _ = l.File.Seek(currentPos, 0)
				return rec, nil
			}

			// the id's match so we should append the contents of the next record onto the contents of the previous record
			// and repeat the process
			impl, ok := rec.(*recordImpl)
			if ok == true {
				log.Printf("INFO: identified additional marc record for %s, appending it", id)
				impl.RawBytes = append(rec.Raw(), nextRec.Raw()...)
			} else {
				log.Printf("ERROR: unable to append MARC additional record")
				_, _ = l.File.Seek(currentPos, 0)
				return rec, nil
			}
		}
	}

	return rec, nil
}

func (l *recordLoaderImpl) Done() {

	if l.File != nil {
		l.File.Close()
		l.File = nil
	}
}

func (l *recordLoaderImpl) Source() string {
	return l.DataSource
}

func (l *recordLoaderImpl) rawMarcRead() (Record, error) {

	// read the 5 byte length header
	_, err := l.File.Read(l.HeaderBuff)
	if err != nil {
		return nil, err
	}

	// is it potentially a record length?
	length, err := strconv.Atoi(string(l.HeaderBuff))
	if err != nil {
		return nil, err
	}

	// ensure the number is sane
	if length <= marcRecordHeaderSize {
		log.Printf("ERROR: marc record prefix invalid (%s)", string(l.HeaderBuff))
		return nil, ErrBadRecord
	}

	// we need to include the header in the raw record so move back so we read it again
	_, err = l.File.Seek(int64(-marcRecordHeaderSize), 1)
	if err != nil {
		return nil, err
	}

	readBuf := make([]byte, length)
	readBytes, err := l.File.Read(readBuf)
	if err != nil {
		return nil, err
	}

	// we did not read the number of bytes we expected, log it and declare victory
	if readBytes != length {
		log.Printf("WARNING: short record read. Expected %d, got %d. Declaring EOF", length, readBytes)
		return nil, io.EOF
	}

	// verify the end of record marker exists and return success if it does
	if readBuf[length-2] == fieldTerminator && readBuf[length-1] == recordTerminator {
		return &recordImpl{RawBytes: readBuf, source: l.DataSource}, nil
	}

	log.Printf("WARNING: unexpected marc record suffix. Expected (%x %x) got (%x %x). Header length reports %d", fieldTerminator, recordTerminator, readBuf[length-2], readBuf[length-1], length)

	//
	// we have a badly formed record, look to see if the terminator appears earlier in the record (in bytes we have already read)
	//

	tag := []byte{fieldTerminator, recordTerminator}
	foundIx := bytes.Index(readBuf, tag)
	if foundIx != -1 {
		log.Printf("WARNING: located record terminator earlier in the buffer at offset %d", foundIx)
		// FIXME: we need to reset the file pointer
		log.Printf("ERROR: WE HAVE NOT RESET THE FILE POINTER, SUBSEQUENT READS WILL BE BAD")
		return &recordImpl{RawBytes: readBuf[0:foundIx], source: l.DataSource}, nil
	}

	//
	// we did not find the record terminator in the buffer we have already read,
	// now look to see if it appears later (because this is a record larger than 5 digits)
	// hate how Hathi does this, should probably follow the Sirsi model instead
	//

	additionalBuffer := make([]byte, 0, 128*1024)
	for {
		b := make([]byte, 1)
		_, err = l.File.Read(b)
		if err != nil {
			log.Printf("ERROR: reading forward for record terminator, giving up (%s)", err.Error())
			break
		}

		additionalBuffer = append(additionalBuffer, b...)

		// did we find the record terminator
		if b[0] == recordTerminator {
			log.Printf("WARNING: record terminator located after an additional %d bytes", len(additionalBuffer))
			return &recordImpl{RawBytes: append(readBuf, additionalBuffer...), source: l.DataSource}, nil
		}
	}

	//log.Printf("FIXME: %s", string(readBuf))
	return nil, ErrBadRecord
}

func (r *recordImpl) Id() (string, error) {

	if r.marcId != "" {
		return r.marcId, nil
	}

	return r.extractId()
}

func (r *recordImpl) Raw() []byte {
	return r.RawBytes
}

func (r *recordImpl) Source() string {
	return r.source
}

func (r *recordImpl) SetSource(source string) {
	r.source = source
}

func (r *recordImpl) extractId() (string, error) {

	id, err := r.getMarcFieldId("001")
	if err != nil {
		id, err = r.getMarcFieldId("035")
	}

	if err != nil {
		return "", err
	}

	// ensure the first character of the Id us a 'u' character
	//if id[0] != 'u' {
	//	log.Printf("ERROR: marc record id is suspect (%s)", id)
	//	return "", BadRecordIdError
	//}

	r.marcId = id

	//log.Printf( "ID: %s", r.marcId )
	return r.marcId, nil
}

//
// A marc record consists of a 5 byte length header (in ascii) followed by a 'directory' of fields.
// The offset of the end of the directory is specified at byte 12 for 5 bytes.
// All field descriptors within the directory consist of the following:
//    field Id (string) bytes 0 - 2 (3 bytes)
//    field length (string) bytes 3 - 6 (4 bytes)
//    field offset (string) bytes 7 - 11 (5 bytes)
//
// The actual field values begin after the end of the directory.
//

func (r *recordImpl) getMarcFieldId(fieldId string) (string, error) {

	currentOffset := marcRecordFieldDirStart
	endOfDir, err := strconv.Atoi(string(r.RawBytes[12:17]))
	if err != nil {
		log.Printf("ERROR: marc record end of directory offset invalid (%s)", string(r.RawBytes[12:17]))
		return "", ErrBadRecord
	}

	// make sure we are actually pointing where we expect
	if endOfDir == 99999 || r.RawBytes[endOfDir-1 : endOfDir][0] != fieldTerminator {
		tag := []byte{fieldTerminator}
		foundIx := bytes.Index(r.RawBytes, tag)
		if foundIx == -1 {
			log.Printf("ERROR: cannot locate end of directory marker")
			return "", ErrBadRecord
		}
		foundIx++
		log.Printf("INFO: resetting directory terminator. was %d, now %d", endOfDir, foundIx)
		endOfDir = foundIx
	}

	for currentOffset < endOfDir {
		dirEntry := r.RawBytes[currentOffset : currentOffset+marcRecordFieldDirEntrySize]
		//log.Printf( "Dir entry [%s]", string( dirEntry ) )
		fieldNumber := string(dirEntry[0:3])
		next := string(dirEntry[3:7])
		fieldLength, err := strconv.Atoi(next)
		if err != nil {
			log.Printf("ERROR: marc record field length invalid (%s)", next)
			return "", ErrBadRecord
		}
		next = string(dirEntry[7:12])
		fieldOffset, err := strconv.Atoi(next)
		if err != nil {
			log.Printf("ERROR: marc record field offset invalid (%s)", next)
			return "", ErrBadRecord
		}

		//log.Printf( "Found field number %s. Offset %d, Length %d", fieldNumber, fieldOffset, fieldLength )
		if fieldNumber == fieldId {
			fieldStart := endOfDir + fieldOffset
			return string(r.RawBytes[fieldStart : fieldStart+fieldLength-1]), nil
		}
		currentOffset += marcRecordFieldDirEntrySize
	}

	log.Printf("ERROR: could not locate field %s in marc record", fieldId)
	return "", ErrBadRecord
}

//
// end of file
//
