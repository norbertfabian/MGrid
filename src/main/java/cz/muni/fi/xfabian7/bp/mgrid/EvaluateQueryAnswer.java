/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cz.muni.fi.xfabian7.bp.mgrid;

import java.io.Serializable;
import java.util.Iterator;
import messif.objects.util.RankedAbstractObject;
import messif.operations.QueryOperation;

/**
 * Return a new instance of Iterator with the answer
 * 
 * @author Norbert Fabian, 396035@mail.muni.cz, Faculty of Informatics, Masaryk
 * University, Brno, Czech Republic\
 */
class EvaluateQueryAnswer implements Iterable<RankedAbstractObject>, Serializable {

    private final QueryOperation queryOperation;
 
    public EvaluateQueryAnswer(QueryOperation queryOperationMine) {
        this.queryOperation = queryOperationMine;
    }

    @Override
    public Iterator<RankedAbstractObject> iterator() {
        System.out.println("EvaluateQueryAnswer: "+queryOperation);
        return queryOperation.getAnswer();
    }
}
