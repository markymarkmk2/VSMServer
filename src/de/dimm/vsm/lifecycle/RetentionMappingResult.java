/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.lifecycle;

import java.io.Serializable;
import javax.persistence.Entity;
import javax.persistence.EntityResult;
import javax.persistence.FieldResult;
import javax.persistence.Id;
import javax.persistence.SqlResultSetMapping;

/**
 *
 * @author Administrator
 */
@SqlResultSetMapping(name = "RetentionResults",
entities =
{
    @EntityResult(entityClass = de.dimm.vsm.lifecycle.RetentionMappingResult.class, fields =
    {
        @FieldResult(name = "fidx", column = "fidx"),
        @FieldResult(name = "fname", column = "fname"),
        @FieldResult(name = "fsize", column = "fsize"),
        @FieldResult(name = "mtime", column = "mtime")
    })
}
//,
//columns =
//{
//    @ColumnResult(name = "fidx"),
//    @ColumnResult(name = "size"),
//    @ColumnResult(name = "mtime")
//}
)

@Entity
public class RetentionMappingResult  implements Serializable
{

    @Id
    long fidx;
    String fname;
    long fsize;
    long mtime;

    @Override
    public String toString()
    {
        return "F: " + fidx + "\tN: " + fname + "\tM: " + mtime + "\tS: " + fsize;
    }

    public long getFidx()
    {
        return fidx;
    }

    public void setFidx( long fidx )
    {
        this.fidx = fidx;
    }
}