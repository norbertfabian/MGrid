/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cz.muni.fi.xfabian7.bp.mgrid;

import messif.objects.util.RankedAbstractObject;
import messif.operations.QueryOperation;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

/**
 * This class is a function to lunch the evaluation of the query on addresses
 * given by the JRDD
 * 
 * @author Norbert Fabian, 396035@mail.muni.cz, Faculty of Informatics, Masaryk
 * University, Brno, Czech Republic\
 */

class EvaluateQuery implements Function<Tuple2<QueryOperation,String>, Iterable<RankedAbstractObject>> {

    private QueryOperation queryOperation;

    @Override
    public Iterable<RankedAbstractObject> call(Tuple2<QueryOperation, String> rdd) throws Exception {

        try {
           queryOperation = rdd._1().clone();

            HdfsStorageBucket hdfsStorage = new HdfsStorageBucket(Integer.MAX_VALUE,
                    8, 0, false, rdd._2());

            System.out.println("EvaluateQUery.call: Orig answer objects=" + queryOperation.getAnswerCount() + ", thread=" + Thread.currentThread().getId() + ", me=" + this);
            QueryOperation queryOperationMine = queryOperation.clone(false);
            
            queryOperationMine.evaluate(hdfsStorage.getAllObjects());
            System.out.println("EvaluateQuery.call: New answer objects=" + queryOperationMine.getAnswerCount() + ", thread=" + Thread.currentThread().getId() + ", me=" + this);
            return new EvaluateQueryAnswer(queryOperationMine);
        } catch (Exception e) {
            System.out.println("Exception error: " + e);
            return null;
        }
    }

}
