`timescale 1ns/1ps
module tb;
    logic clk = 0;
    logic reset = 1;
    logic [7:0] count = 0;
    logic [7:0] expected = 0;
    integer fd;

    always #5 clk = ~clk;
    always @(posedge clk)
        if (reset) count <= 0;
        else count <= count + 8'd1;

    initial begin
        #12;
        @(negedge clk) reset = 0;
        repeat (4096) begin
            @(negedge clk);
            expected = expected + 8'd1;
            assert (count == expected) else $fatal(1, "counter mismatch");
        end
        fd = $fopen("metrics.json", "w");
        $fwrite(fd, "{\"cycles\":4096,\"passed\":true}\n");
        $fclose(fd);
        $display("PASS counter 4096 cycles");
        $finish;
    end
endmodule
