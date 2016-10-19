package no.kantega.mlworkshop.task3;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.function.Function;
import org.nd4j.linalg.dataset.DataSet;
import scala.Tuple2;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class FromSequenceFilePairFunction implements Function<Tuple2<Text,BytesWritable>,DataSet> {

    public DataSet call(Tuple2<Text, BytesWritable> v1) throws Exception {
        DataSet ds = new DataSet();
        ByteArrayInputStream bais = new ByteArrayInputStream(v1._2().getBytes());
        ds.load(bais);
        return ds;
    }



    public Iterator<DataSet> call2(Iterator<Tuple2<Text, BytesWritable>> iterator) throws Exception {
        List<DataSet> out = new ArrayList<>();
        while(iterator.hasNext()){
            DataSet ds = new DataSet();
            ByteArrayInputStream bais = new ByteArrayInputStream(iterator.next()._2().getBytes());
            ds.load(bais);
            out.add(ds);
        }

        return out.iterator();
    }
}
