import os
import sys
import glob
import shutil
import subprocess
import multiprocessing

from time import perf_counter
from functools import partial


def main():
    print()
    cores = multiprocessing.cpu_count()
    print(f'Available cores: {cores}')

    mode, bf_size, input_folder, input_shps, output_folder = ask_inputs()

    if mode == '1':
        t1_start = perf_counter()
        run_single(input_shps, output_folder, bf_size)
    elif mode == '2':
        t1_start = perf_counter()
        results = None
        # https://youtu.be/PcJZeCEEhws
        with multiprocessing.Pool(processes=cores) as pool:
            results = pool.map(
                partial(run_multi, output_folder=output_folder, bf_size=bf_size), input_shps)
        # run_multi(input_shps, output_folder, bf_size)
        
    t1_stop = perf_counter()
    print("Elapsed time:", t1_stop-t1_start)


def ask_inputs():
    while True:
        mode = input(
            '\nType <1> for single-threaded processing or <2> for multiprocessing and press <Enter>\n')
        if mode == '1' or mode == '2':
            break
        else:
            print('\n*** Invalid option')
            continue

    while True:
        bf_size = input('\nType buffer size:\n')
        try:
            bf_size_int = int(bf_size)
            break
        except Exception:
            print('*** Invalid input')
            continue

    while True:
        input_folder = input('\nType input folder path:\n')
        if os.path.isdir(input_folder):
            input_shps = glob.glob(os.path.join(input_folder, '*.shp'))
            if len(input_shps) == 0:
                print('\n*** No shapefiles found')
                continue
            else:
                output_folder = os.path.join(input_folder, 'outputs')
                if os.path.exists(output_folder):
                    shutil.rmtree(output_folder)
                os.makedirs(output_folder)
                break
        else:
            print('\n*** Invalid input folder')
            continue

    return mode, bf_size, input_folder, input_shps, output_folder


def run_single(input_shps, output_folder, bf_size):
    cmd = ['ogr2ogr', '-f', 'ESRI Shapefile', '-dialect', 'SQLite',
           '-sql', 'select ST_Buffer(geometry, !bf_size!) from $shp_name$']

    print()
    print(f'Creating buffers... (size: {bf_size})')

    for i, shp in enumerate(input_shps):
        filename = os.path.basename(shp)
        filename_no_ext = os.path.splitext(filename)[0]

        cmd2run = [x.replace('$shp_name$', filename_no_ext).replace(
            '!bf_size!', bf_size) for x in cmd]

        output_filename = os.path.join(
            output_folder, f'{filename_no_ext}_buffered_{bf_size}.shp')

        cmd2run.append(output_filename)
        cmd2run.append(shp)

        p = subprocess.Popen(cmd2run)
        p.wait()

        progress(i+1, len(input_shps))

    print()


def run_multi(input_shp, output_folder, bf_size):
    # print('*** Not implemented yet.')

    cmd = ['ogr2ogr', '-f', 'ESRI Shapefile', '-dialect', 'SQLite',
           '-sql', 'select ST_Buffer(geometry, !bf_size!) from $shp_name$']

    print()
    print(f'Creating buffers... (size: {bf_size})')

    filename = os.path.basename(input_shp)
    filename_no_ext = os.path.splitext(filename)[0]
    cmd2run = [x.replace('$shp_name$', filename_no_ext).replace(
        '!bf_size!', bf_size) for x in cmd]
    output_filename = os.path.join(
        output_folder, f'{filename_no_ext}_buffered_{bf_size}.shp')
    cmd2run.append(output_filename)
    cmd2run.append(input_shp)
    p = subprocess.Popen(cmd2run)
    p.wait()


# Progress bar
def progress(count, total, suffix=''):
    # https://stackoverflow.com/a/27871113
    bar_len = 50
    filled_len = int(round(bar_len * count / float(total)))

    percents = round(100.0 * count / float(total), 1)
    bar = '#' * filled_len + '-' * (bar_len - filled_len)

    sys.stdout.write('[%s] %s%s complete%s\r' % (bar, percents, '%', suffix))
    sys.stdout.flush()


if __name__ == '__main__':
    main()
