function get_formatted_date_from_now(time_in_ms: number, future_date: Date) {

    const month = String(future_date.getMonth() + 1).padStart(2, '0'); // Months are 0-based
    const day = String(future_date.getDate()).padStart(2, '0');
    const year = future_date.getFullYear();

    let hours = future_date.getHours();
    const minutes = String(future_date.getMinutes()).padStart(2, '0');
    const ampm = hours >= 12 ? 'PM' : 'AM';
    hours = hours % 12 || 12; // Convert to 12-hour format, setting 0 to 12

    return `${month}/${day}/${year} ${hours}:${minutes} ${ampm}`;
}

/**
 * @description Delays the restart of the bot initialization given a set time.  
*/
export async function delay_start(time_in_ms: number, function_to_run: Function) {

    const future_date = new Date(Date.now() + time_in_ms);

    console.log('-------------------------------------');
    console.log(`Standing by. Will resume in ~${time_in_ms / 1000 / 60} minutes, around ${get_formatted_date_from_now(time_in_ms, future_date)}`);

    // Provides an update log every 15 minutes
    const update_interval = setInterval(() => {

        console.log('-------------------------------------');
        console.log(`Standing by. Will resume in ~${time_in_ms / 1000 / 60} minutes, around ${get_formatted_date_from_now(time_in_ms, future_date)}`);

    }, 15 * 60000)

    setTimeout(() => {

        clearInterval(update_interval);

        console.log("Delay expired. Restarting...");

        function_to_run();

    }, time_in_ms)

}