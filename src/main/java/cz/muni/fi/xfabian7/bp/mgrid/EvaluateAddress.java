/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cz.muni.fi.xfabian7.bp.mgrid;

import cz.muni.fi.xfabian7.bp.mgrid.dindex.DIndex;
import java.util.ArrayList;
import java.util.List;
import messif.operations.QueryOperation;
import messif.operations.query.RangeQueryOperation;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

/**
 * This class lunch the evaluation of relevant addresses on the query given
 * through JRDD
 *
 * @author Norbert Fabian, 396035@mail.muni.cz, Faculty of Informatics, Masaryk
 * University, Brno, Czech Republic\
 */

class EvaluateAddress implements PairFlatMapFunction<QueryOperation, QueryOperation, String> {

    private final DIndex dIndex;
    public static final String FILE_PATH = "hdfs://localhost:9000/HdfsBucket";

    public EvaluateAddress(DIndex dIndex) {
        this.dIndex = dIndex;
    }

    @Override
    public Iterable<Tuple2<QueryOperation, String>> call(QueryOperation query) throws Exception {
        List<Integer> addrInt = new ArrayList<>();
        QueryOperation emptyQueryOperation;
        emptyQueryOperation = query.clone();

        ArrayList<Tuple2<QueryOperation, String>> res
                = new ArrayList<Tuple2<QueryOperation, String>>();

        addrInt = dIndex.rangeSearch((RangeQueryOperation) query);

        for (Integer i : addrInt) {
            String s = MGrid.FILE_PATH + i;
            res.add(new Tuple2<>(emptyQueryOperation, s));
        }

        Iterable<Tuple2<QueryOperation, String>> iterable;
        iterable = res;
        return iterable;

    }
}
