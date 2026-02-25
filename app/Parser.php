<?php

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $fileSize = filesize($inputPath);

        // Find midpoint at a newline boundary
        $handle = fopen($inputPath, 'r');
        fseek($handle, (int)($fileSize / 2));
        fgets($handle);
        $midPoint = ftell($handle);
        fclose($handle);

        // Fork child for second half - use /dev/shm if available (RAM-backed)
        $tmpDir = is_dir('/dev/shm') ? '/dev/shm' : sys_get_temp_dir();
        $tmpFile = $tmpDir . '/parse_' . getmypid();
        $pid = pcntl_fork();

        if ($pid === 0) {
            $result = $this->processRange($inputPath, $midPoint, $fileSize);
            file_put_contents($tmpFile, igbinary_serialize($result));
            exit(0);
        }

        // Parent: process first half
        $data = $this->processRange($inputPath, 0, $midPoint);

        // Wait for child and merge
        pcntl_waitpid($pid, $status);
        $childData = igbinary_unserialize(file_get_contents($tmpFile));
        unlink($tmpFile);

        foreach ($childData as $key => $count) {
            if (isset($data[$key])) {
                $data[$key] += $count;
            } else {
                $data[$key] = $count;
            }
        }
        unset($childData);

        $this->writeOutput($data, $outputPath);
    }

    private function processRange(string $inputPath, int $startPos, int $endPos): array
    {
        $handle = fopen($inputPath, 'r');
        if ($startPos > 0) {
            fseek($handle, $startPos);
        }

        $data = [];
        $pos = $startPos;

        while ($pos < $endPos && ($line = fgets($handle)) !== false) {
            $pos += strlen($line);
            $key = substr($line, 19, -16);
            if (isset($data[$key])) {
                $data[$key]++;
            } else {
                $data[$key] = 1;
            }
        }

        fclose($handle);
        return $data;
    }

    private function writeOutput(array $data, string $outputPath): void
    {
        $pathOrder = [];
        $pathSeen = [];
        $byPath = [];
        foreach ($data as $key => $count) {
            $path = substr($key, 0, -11);
            $date = substr($key, -10);
            if (!isset($pathSeen[$path])) {
                $pathSeen[$path] = 1;
                $pathOrder[] = $path;
            }
            $byPath[$path][$date] = $count;
        }

        $out = fopen($outputPath, 'w');
        stream_set_write_buffer($out, 1 << 20);
        $buf = "{\n";
        $firstPath = true;
        foreach ($pathOrder as $path) {
            $dates = $byPath[$path];
            ksort($dates);

            if (!$firstPath) {
                $buf .= ",\n";
            }
            $escapedPath = str_replace('/', '\\/', $path);
            $buf .= "    \"$escapedPath\": {\n";
            $firstDate = true;
            foreach ($dates as $date => $count) {
                if (!$firstDate) {
                    $buf .= ",\n";
                }
                $buf .= "        \"$date\": $count";
                $firstDate = false;
            }
            $buf .= "\n    }";
            $firstPath = false;

            if (strlen($buf) > (1 << 20)) {
                fwrite($out, $buf);
                $buf = '';
            }
        }
        $buf .= "\n}";
        fwrite($out, $buf);
        fclose($out);

    }
}
