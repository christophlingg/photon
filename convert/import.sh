for file in x*
do
        echo importing $file ...
        curl --silent --show-error -XPOST localhost:9200/photon/place/_bulk  --data-binary @$file >  /dev/null
done

# snippet for splitting files:
# split -a 3 -l 150000 file
