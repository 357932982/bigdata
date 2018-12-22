package com.bigdata.mapreduce.orderSort;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class OrderBean  implements WritableComparable<OrderBean> {

    private Text itemId;
    private DoubleWritable amount;


    public void set(Text itemId, DoubleWritable amount) {
        this.itemId = itemId;
        this.amount = amount;
    }

    @Override
    public int compareTo(OrderBean o) {
        int cmp = this.itemId.compareTo(o.getItemId());
        if (cmp == 0){
            cmp = -this.amount.compareTo(o.getAmount());
        }
        return cmp;
    }

    @Override
    public void write(DataOutput out) throws IOException {

        out.writeUTF(itemId.toString());
        out.writeDouble(amount.get());
    }

    @Override
    public void readFields(DataInput in) throws IOException {

        String readUTF = in.readUTF();
        double readDouble = in.readDouble();

        this.itemId = new Text(readUTF);
        this.amount = new DoubleWritable(readDouble);
    }

    @Override
    public String toString() {

        return itemId.toString() + "\t" + amount.get();

    }
}
