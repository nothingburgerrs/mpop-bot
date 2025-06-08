const {
    Client,
    GatewayIntentBits,
    SlashCommandBuilder,
    EmbedBuilder,
    PermissionFlagsBits,
} = require("discord.js");
const fs = require("fs").promises;
const path = require("path");

// Keep alive server
require("./keep_alive");

// Bot configuration
const client = new Client({
    intents: [
        GatewayIntentBits.Guilds,
        GatewayIntentBits.GuildMessages,
        GatewayIntentBits.MessageContent,
    ],
});

// Data storage
let botData = {
    companies: {},
    groups: {},
    users: {},
    charts: {
        MelOn: {},
        Genies: {},
        Bugs: {},
        FLO: {},
    },
    previousCharts: { // To track movement
        MelOn: {},
        Genies: {},
        Bugs: {},
        FLO: {},
    },
    mentions: {},
    lastMentionReset: 0,
    lastChartReset: 0, // New: To track last chart reset
};

// Constants
const COMPANY_SIZES = {
    small: { baseFunds: 50000, debutPopularity: [50, 500], maxFunds: 200000 },
    medium: { baseFunds: 150000, debutPopularity: [800, 3000], maxFunds: 500000 },
    big: { baseFunds: 500000, debutPopularity: [2500, 8000], maxFunds: 2000000 },
};

const MUSIC_SHOWS = [
    "Music Bank",
    "Show!Music Core",
    "M COUNTDOWN",
    "Inkigayo",
    "The Show",
];
const CHART_PLATFORMS = ["MelOn", "Genies", "Bugs", "FLO"];

// Utility functions
function getRandomInt(min, max) {
    return Math.floor(Math.random() * (max - min + 1)) + min;
}

function calculateGroupTier(popularity, fans) {
    if (popularity >= 50000 && fans >= 200000) return "icon";
    if (popularity >= 20000 && fans >= 80000) return "big";
    if (popularity >= 5000 && fans >= 20000) return "medium";
    if (popularity >= 1000 && fans >= 3000) return "small";
    return "nugu";
}

function generateSales(groupTier, popularity) {
    const baseSales = {
        nugu: getRandomInt(10, 100),
        small: getRandomInt(100, 1000),
        medium: getRandomInt(1000, 10000),
        big: getRandomInt(10000, 100000),
        icon: getRandomInt(50000, 500000),
    };

    const popularityMultiplier = Math.max(1, popularity / 1000);
    return Math.floor(baseSales[groupTier] * popularityMultiplier);
}

function updateCharts() {
    // Save current charts as previous for position indicators
    botData.previousCharts = JSON.parse(JSON.stringify(botData.charts));
    
    // Clear current charts for a fresh update
    CHART_PLATFORMS.forEach(platform => {
        botData.charts[platform] = {};
    });

    // Get all groups
    const allGroups = Object.values(botData.groups);

    allGroups.forEach((group) => {
        // Charts are now based on group's overall performance/popularity, not specific albums
        // Combine popularity, fans, mentions, and potentially skills for a "chart score"
        const groupChartScore = 
            group.popularity * 0.5 + 
            group.fans * 0.3 + 
            (botData.mentions[group.name] || 0) * 0.2 +
            (group.skills.vocal + group.skills.dance + group.skills.rap) / 3 * 10; // Skills add a small boost

        if (groupChartScore <= 0) return; // Don't chart groups with no score

        CHART_PLATFORMS.forEach((platform) => {
            const groupTier = calculateGroupTier(group.popularity, group.fans);
            const chartingData = {
                nugu: { chance: 0.05, positions: [90, 100] }, // Very low chance
                small: { chance: 0.2, positions: [70, 100] },
                medium: { chance: 0.6, positions: [30, 80] },
                big: { chance: 0.9, positions: [5, 50] },
                icon: { chance: 0.98, positions: [1, 20] },
            }[groupTier];

            // Only consider groups for charting if they have a decent score
            if (groupChartScore > 100 && Math.random() < chartingData.chance) {
                let position = getRandomInt(chartingData.positions[0], chartingData.positions[1]);

                // Adjust position based on groupChartScore (higher score = lower position number)
                const scoreInfluence = Math.floor(groupChartScore / 200); // Tweak this value
                position = Math.max(1, position - scoreInfluence);

                // Ensure unique positions for the top of the charts
                const currentChartValues = Object.values(botData.charts[platform]);
                while (currentChartValues.includes(position) && position <= 100) {
                    position = getRandomInt(1, 100); // Find a new random position
                }
                
                if (position <= 100) {
                    botData.charts[platform][group.name] = position;
                }
            }
        });
    });

    // Sort charts by position after all groups have been added
    CHART_PLATFORMS.forEach(platform => {
        const sortedChart = Object.entries(botData.charts[platform])
            .sort((a, b) => a[1] - b[1])
            .reduce((acc, [key, value]) => {
                acc[key] = value;
                return acc;
            }, {});
        botData.charts[platform] = sortedChart;
    });
}

function getPositionIndicator(groupName, platform) {
    const currentPos = botData.charts[platform][groupName];
    const previousPos = botData.previousCharts[platform][groupName];
    
    if (currentPos === undefined) return ""; // Not charting anymore

    if (previousPos === undefined) return " 🆕"; // New entry
    if (currentPos < previousPos) return ` (+${previousPos - currentPos})`; // Moved up
    if (currentPos > previousPos) return ` (-${currentPos - previousPos})`; // Moved down
    return " ▬"; // No change
}

function checkPAK(groupName) {
    const group = botData.groups[groupName];
    if (!group) return false;

    let hasAllNumber1 = true;
    CHART_PLATFORMS.forEach((platform) => {
        const platformChart = botData.charts[platform];
        let foundGroupAtNumber1 = false;
        // Check if the current group is exactly at #1 for this platform
        if (platformChart[groupName] === 1) {
            foundGroupAtNumber1 = true;
        }
        if (!foundGroupAtNumber1) {
            hasAllNumber1 = false;
        }
    });
    return hasAllNumber1;
}


// Data persistence
async function saveData() {
    try {
        await fs.writeFile("botData.json", JSON.stringify(botData, null, 2));
    } catch (error) {
        console.error("Error saving data:", error);
    }
}

async function loadData() {
    try {
        const data = await fs.readFile("botData.json", "utf8");
        botData = JSON.parse(data);
        
        // Initialize previousCharts if it doesn't exist or is not in the correct format
        if (!botData.previousCharts || Object.keys(botData.previousCharts).length === 0) {
            botData.previousCharts = {
                MelOn: {},
                Genies: {},
                Bugs: {},
                FLO: {},
            };
        }
        // Initialize charts if it doesn't exist
        if (!botData.charts) {
            botData.charts = {
                MelOn: {},
                Genies: {},
                Bugs: {},
                FLO: {},
            };
        }
        
        // Initialize social media for existing groups if they don't have it
        Object.values(botData.groups).forEach(group => {
            if (!group.socialMedia) {
                group.socialMedia = {
                    Titter: Math.floor(group.followers * 0.8),
                    TikTak: Math.floor(group.followers * 1.2),
                    YouTuboo: Math.floor(group.followers * 0.6),
                    Isutagram: Math.floor(group.followers),
                };
            }
            // Ensure skills object is initialized
            if (!group.skills) {
                group.skills = { rap: 50, vocal: 50, dance: 50 };
            }
            // Ensure wins and paks are initialized
            if (typeof group.wins !== 'number') {
                group.wins = 0;
            }
            if (typeof group.paks !== 'number') {
                group.paks = 0;
            }
            // Ensure keywords are initialized
            if (!group.keywords || !Array.isArray(group.keywords)) {
                group.keywords = [group.name.toLowerCase()];
            } else if (!group.keywords.includes(group.name.toLowerCase())) {
                 group.keywords.push(group.name.toLowerCase());
            }

            // Ensure popularity, fans, followers are numbers and not NaN
            group.popularity = Number(group.popularity) || 0;
            group.fans = Number(group.fans) || 0;
            group.followers = Number(group.followers) || 0;
            for (const platform in group.socialMedia) {
                group.socialMedia[platform] = Number(group.socialMedia[platform]) || 0;
            }
        });

        // Initialize lastStream for existing users
        Object.values(botData.users).forEach(user => {
            if (typeof user.balance !== 'number' || isNaN(user.balance)) {
                user.balance = 1000; // Reset or default if NaN
            }
            if (!user.lastWork) {
                user.lastWork = 0;
            }
            if (!user.lastDaily) {
                user.lastDaily = 0;
            }
            if (!user.lastStream) {
                user.lastStream = 0;
            }
        });

        // Add owner field to existing companies (set to null for existing ones without owner)
        Object.values(botData.companies).forEach(company => {
            if (!company.owner) {
                company.owner = null;
            }
            // Ensure funds is a number and not NaN
            company.funds = Number(company.funds) || 0;
            if (!Array.isArray(company.groups)) {
                company.groups = []; // Ensure groups is an array
            }
        });

        // Initialize lastMentionReset and lastChartReset
        if (typeof botData.lastMentionReset !== 'number') {
            botData.lastMentionReset = 0;
        }
        if (typeof botData.lastChartReset !== 'number') {
            botData.lastChartReset = 0;
        }

    } catch (error) {
        console.log("No existing data file found or error loading data, starting fresh:", error.message);
        // Ensure initial structure is clean if loading fails
        botData = {
            companies: {},
            groups: {},
            users: {},
            charts: {
                MelOn: {},
                Genies: {},
                Bugs: {},
                FLO: {},
            },
            previousCharts: {
                MelOn: {},
                Genies: {},
                Bugs: {},
                FLO: {},
            },
            mentions: {},
            lastMentionReset: 0,
            lastChartReset: 0,
        };
    }
}

// Slash commands
const commands = [
    new SlashCommandBuilder()
        .setName("help")
        .setDescription("List all available commands"),

    new SlashCommandBuilder()
        .setName("addcompany")
        .setDescription("Add a new company")
        .addStringOption((option) =>
            option
                .setName("name")
                .setDescription("Company name")
                .setRequired(true),
        )
        .addStringOption((option) =>
            option
                .setName("size")
                .setDescription("Company size")
                .setRequired(true)
                .addChoices(
                    { name: "Small", value: "small" },
                    { name: "Medium", value: "medium" },
                    { name: "Big", value: "big" },
                ),
        ),

    new SlashCommandBuilder()
        .setName("companyfunds")
        .setDescription("Check company funds")
        .addStringOption((option) =>
            option
                .setName("company")
                .setDescription("Company name")
                .setRequired(true),
        ),

    new SlashCommandBuilder()
        .setName("fundcompany")
        .setDescription("Send money to a company")
        .addStringOption((option) =>
            option
                .setName("company")
                .setDescription("Company name")
                .setRequired(true),
        )
        .addIntegerOption((option) =>
            option
                .setName("amount")
                .setDescription("Amount to send")
                .setRequired(true),
        ),

    new SlashCommandBuilder()
        .setName("work")
        .setDescription("Work to earn money"),

    new SlashCommandBuilder()
        .setName("daily")
        .setDescription("Claim daily bonus"),

    new SlashCommandBuilder()
        .setName("balance")
        .setDescription("Check your balance"),

    new SlashCommandBuilder()
        .setName("debut")
        .setDescription("Debut a new group")
        .addStringOption((option) =>
            option
                .setName("groupname")
                .setDescription("Group name")
                .setRequired(true),
        )
        .addStringOption((option) =>
            option
                .setName("albumname")
                .setDescription("Debut album name")
                .setRequired(true),
        )
        .addStringOption((option) =>
            option
                .setName("company")
                .setDescription("Company name")
                .setRequired(true),
        )
        .addIntegerOption((option) =>
            option
                .setName("investment")
                .setDescription("Investment amount")
                .setRequired(true),
        ),

    new SlashCommandBuilder()
        .setName("comeback")
        .setDescription("Group comeback")
        .addStringOption((option) =>
            option
                .setName("groupname")
                .setDescription("Group name")
                .setRequired(true),
        )
        .addStringOption((option) =>
            option
                .setName("albumname")
                .setDescription("Album name")
                .setRequired(true),
        )
        .addIntegerOption((option) =>
            option
                .setName("investment")
                .setDescription("Investment amount")
                .setRequired(true),
        ),

    new SlashCommandBuilder()
        .setName("setgroup")
        .setDescription("Set existing group stats (Admin only)")
        .setDefaultMemberPermissions(PermissionFlagsBits.Administrator)
        .addStringOption((option) =>
            option
                .setName("groupname")
                .setDescription("Group name")
                .setRequired(true),
        )
        .addStringOption((option) =>
            option
                .setName("company")
                .setDescription("Company name")
                .setRequired(true),
        )
        .addStringOption((option) =>
            option
                .setName("debutalbum")
                .setDescription("Debut album name")
                .setRequired(true),
        )
        .addStringOption((option) =>
            option
                .setName("comebacks")
                .setDescription("Comeback albums (comma separated)")
                .setRequired(false),
        )
        .addStringOption((option) =>
            option
                .setName("tier")
                .setDescription("Group tier")
                .setRequired(false)
                .addChoices(
                    { name: "Nugu", value: "nugu" },
                    { name: "Small", value: "small" },
                    { name: "Medium", value: "medium" },
                    { name: "Big", value: "big" },
                    { name: "Icon", value: "icon" },
                ),
        )
        .addIntegerOption((option) =>
            option
                .setName("popularity")
                .setDescription("Initial popularity")
                .setRequired(false)
        )
        .addIntegerOption((option) =>
            option
                .setName("fans")
                .setDescription("Initial fans")
                .setRequired(false)
        )
        .addIntegerOption((option) =>
            option
                .setName("wins")
                .setDescription("Number of wins")
                .setRequired(false)
        )
        .addIntegerOption((option) =>
            option
                .setName("paks")
                .setDescription("Number of PAKs")
                .setRequired(false)
        ),


    new SlashCommandBuilder()
        .setName("groups")
        .setDescription("List all groups"),

    new SlashCommandBuilder()
        .setName("groupstats")
        .setDescription("View detailed group statistics")
        .addStringOption((option) =>
            option
                .setName("groupname")
                .setDescription("Group name")
                .setRequired(true),
        ),

    new SlashCommandBuilder()
        .setName("addwin")
        .setDescription("Add a music show win")
        .addStringOption((option) =>
            option
                .setName("groupname")
                .setDescription("Group name")
                .setRequired(true),
        )
        .addStringOption((option) =>
            option
                .setName("musicshow")
                .setDescription("Music show")
                .setRequired(true)
                .addChoices(
                    ...MUSIC_SHOWS.map((show) => ({ name: show, value: show })),
                ),
        ),

    new SlashCommandBuilder()
        .setName("resetmentions")
        .setDescription("Reset all group mentions (Admin only)")
        .setDefaultMemberPermissions(PermissionFlagsBits.Administrator),

    new SlashCommandBuilder()
        .setName("resetcharts")
        .setDescription("Clear all music charts and reset (Admin only)")
        .setDefaultMemberPermissions(PermissionFlagsBits.Administrator),

    new SlashCommandBuilder()
        .setName("brandrep")
        .setDescription("View brand reputation ranking"),

    new SlashCommandBuilder()
        .setName("addkeywords")
        .setDescription("Add keywords for group mentions")
        .addStringOption((option) =>
            option
                .setName("groupname")
                .setDescription("Group name")
                .setRequired(true),
        )
        .addStringOption((option) =>
            option
                .setName("keywords")
                .setDescription("Keywords (comma separated)")
                .setRequired(true),
        ),

    new SlashCommandBuilder()
        .setName("sales")
        .setDescription("Generate album sales")
        .addStringOption((option) =>
            option
                .setName("groupname")
                .setDescription("Group name")
                .setRequired(true),
        ),

    new SlashCommandBuilder()
        .setName("buzz")
        .setDescription("Generate buzz for a group")
        .addStringOption((option) =>
            option
                .setName("groupname")
                .setDescription("Group name")
                .setRequired(true),
        ),

    new SlashCommandBuilder()
        .setName("scandal")
        .setDescription("Generate a scandal (gamble)")
        .addStringOption((option) =>
            option
                .setName("groupname")
                .setDescription("Group name")
                .setRequired(true),
        ),

    new SlashCommandBuilder()
        .setName("performance")
        .setDescription("Group performance")
        .addStringOption((option) =>
            option
                .setName("groupname")
                .setDescription("Group name")
                .setRequired(true),
        ),

    new SlashCommandBuilder()
        .setName("trainvocals")
        .setDescription("Train group vocals")
        .addStringOption((option) =>
            option
                .setName("groupname")
                .setDescription("Group name")
                .setRequired(true),
        ),

    new SlashCommandBuilder()
        .setName("traindance")
        .setDescription("Train group dance")
        .addStringOption((option) =>
            option
                .setName("groupname")
                .setDescription("Group name")
                .setRequired(true),
        ),

    new SlashCommandBuilder()
        .setName("trainrap")
        .setDescription("Train group rap")
        .addStringOption((option) =>
            option
                .setName("groupname")
                .setDescription("Group name")
                .setRequired(true),
        ),

    new SlashCommandBuilder()
        .setName("sponsorship")
        .setDescription("Attempt to land a sponsorship")
        .addStringOption((option) =>
            option
                .setName("groupname")
                .setDescription("Group name")
                .setRequired(true),
        ),

    new SlashCommandBuilder()
        .setName("stream")
        .setDescription("Stream a group album")
        .addStringOption((option) =>
            option
                .setName("groupname")
                .setDescription("Group name")
                .setRequired(true),
        )
        .addStringOption((option) =>
            option
                .setName("albumname")
                .setDescription("Album name")
                .setRequired(true),
        ),

    new SlashCommandBuilder()
        .setName("charts")
        .setDescription("View music charts")
        .addStringOption((option) =>
            option
                .setName("groupname")
                .setDescription("Group name (optional)")
                .setRequired(false),
        ),

    new SlashCommandBuilder()
        .setName("payola")
        .setDescription("Pay for better chart positions")
        .addStringOption((option) =>
            option
                .setName("groupname")
                .setDescription("Group name")
                .setRequired(true),
        )
        .addIntegerOption((option) =>
            option
                .setName("amount")
                .setDescription("Amount to pay (minimum 1,000,000)")
                .setRequired(true),
        ),

    new SlashCommandBuilder()
        .setName("newpost")
        .setDescription("Make a social media post")
        .addStringOption((option) =>
            option
                .setName("groupname")
                .setDescription("Group name")
                .setRequired(true),
        )
        .addStringOption((option) =>
            option
                .setName("platform")
                .setDescription("Social media platform")
                .setRequired(true)
                .addChoices(
                    { name: "Titter", value: "Titter" },
                    { name: "TikTak", value: "TikTak" },
                    { name: "YouTuboo", value: "YouTuboo" },
                    { name: "Isutagram", value: "Isutagram" },
                ),
        ),

    new SlashCommandBuilder()
        .setName("companies")
        .setDescription("List all companies and their groups"),

    new SlashCommandBuilder()
        .setName("buyfollowers")
        .setDescription("Buy social media followers")
        .addStringOption((option) =>
            option
                .setName("groupname")
                .setDescription("Group name")
                .setRequired(true),
        )
        .addStringOption((option) =>
            option
                .setName("platform")
                .setDescription("Social media platform")
                .setRequired(true)
                .addChoices(
                    { name: "Titter", value: "Titter" },
                    { name: "TikTak", value: "TikTak" },
                    { name: "YouTuboo", value: "YouTuboo" },
                    { name: "Isutagram", value: "Isutagram" },
                ),
        )
        .addIntegerOption((option) =>
            option
                .setName("amount")
                .setDescription("Number of followers to buy (minimum 100)")
                .setRequired(true),
        ),
    new SlashCommandBuilder()
        .setName("disbandgroup")
        .setDescription("Disband a group (Company Owner Only)")
        .addStringOption((option) =>
            option
                .setName("groupname")
                .setDescription("The name of the group to disband")
                .setRequired(true)
        ),
    new SlashCommandBuilder()
        .setName("closecompany")
        .setDescription("Close your company (Company Owner Only)")
        .addStringOption((option) =>
            option
                .setName("companyname")
                .setDescription("The name of the company to close")
                .setRequired(true)
        ),
];

// Command handlers
client.on("interactionCreate", async (interaction) => {
    if (!interaction.isChatInputCommand()) return;

    const { commandName, options, user } = interaction;
    const userId = user.id;

    // Initialize user if doesn't exist
    if (!botData.users[userId]) {
        botData.users[userId] = {
            balance: 1000,
            lastWork: 0,
            lastDaily: 0,
            lastStream: 0,
        };
    }

    try {
        switch (commandName) {
            case "help":
                const embed = new EmbedBuilder()
                    .setTitle("K-pop Roleplay Bot Commands")
                    .setColor(0xff69b4)
                    .addFields(
                        {
                            name: "Economy",
                            value: "/work, /daily, /balance, /fundcompany",
                        },
                        {
                            name: "Companies",
                            value: "/addcompany, /companyfunds, /closecompany",
                        },
                        {
                            name: "Groups",
                            value: "/debut, /comeback, /groups, /groupstats, /disbandgroup",
                        },
                        {
                            name: "Activities",
                            value: "/addwin, /sales, /buzz, /scandal, /performance, /sponsorship, /newpost",
                        },
                        {
                            name: "Training",
                            value: "/trainvocals, /traindance, /trainrap",
                        },
                        {
                            name: "Charts & Popularity",
                            value: "/charts, /stream, /payola, /brandrep, /buyfollowers",
                        },
                        {
                            name: "Admin",
                            value: "/resetmentions, /resetcharts, /addkeywords, /setgroup",
                        },
                    );
                await interaction.reply({ embeds: [embed] });
                break;

            case "addcompany":
                const companyName = options.getString("name");
                const companySize = options.getString("size");

                if (botData.companies[companyName]) {
                    await interaction.reply(
                        `❌ Company "${companyName}" already exists!`,
                    );
                    return;
                }

                // Check if user already owns a company
                const userOwnedCompany = Object.values(botData.companies).find(
                    (company) => company.owner === userId
                );
                if (userOwnedCompany) {
                    await interaction.reply(
                        `❌ You already own a company: "${userOwnedCompany.name}"! You can only own one company at a time.`,
                    );
                    return;
                }

                botData.companies[companyName] = {
                    size: companySize,
                    funds: COMPANY_SIZES[companySize].baseFunds,
                    groups: [],
                    owner: userId,
                };

                await interaction.reply(
                    `✅ Company "${companyName}" (${companySize}) created with ${COMPANY_SIZES[companySize].baseFunds.toLocaleString()} :MonthlyPeso:`,
                );
                await saveData();
                break;

            case "companyfunds":
                const checkCompany = options.getString("company");
                if (!botData.companies[checkCompany]) {
                    await interaction.reply(
                        `❌ Company "${checkCompany}" not found!`,
                    );
                    return;
                }

                await interaction.reply(
                    `💰 ${checkCompany} has ${botData.companies[checkCompany].funds.toLocaleString()} :MonthlyPeso:`,
                );
                break;

            case "fundcompany":
                const fundCompany = options.getString("company");
                const fundAmount = options.getInteger("amount");

                if (!botData.companies[fundCompany]) {
                    await interaction.reply(
                        `❌ Company "${fundCompany}" not found!`,
                    );
                    return;
                }

                if (botData.users[userId].balance < fundAmount) {
                    await interaction.reply("❌ Insufficient funds!");
                    return;
                }
                if (fundAmount <= 0) {
                    await interaction.reply("❌ Amount must be positive!");
                    return;
                }

                botData.users[userId].balance -= fundAmount;
                botData.companies[fundCompany].funds += fundAmount;

                await interaction.reply(
                    `✅ Sent ${fundAmount.toLocaleString()} :MonthlyPeso: to ${fundCompany}`,
                );
                await saveData();
                break;

            case "work":
                const now = Date.now();
                const lastWork = botData.users[userId].lastWork || 0;

                if (now - lastWork < 3600000) {
                    // 1 hour cooldown
                    const remaining = Math.ceil(
                        (3600000 - (now - lastWork)) / 60000,
                    );
                    await interaction.reply(
                        `⏰ You can work again in ${remaining} minutes!`,
                    );
                    return;
                }

                const workEarnings = getRandomInt(100, 500);
                botData.users[userId].balance += workEarnings;
                botData.users[userId].lastWork = now;

                await interaction.reply(
                    `💼 You worked and earned ${workEarnings} :MonthlyPeso:!`,
                );
                await saveData();
                break;

            case "daily":
                const dailyNow = Date.now();
                const lastDaily = botData.users[userId].lastDaily || 0;

                if (dailyNow - lastDaily < 86400000) {
                    // 24 hour cooldown
                    const remaining = Math.ceil(
                        (86400000 - (dailyNow - lastDaily)) / 3600000,
                    );
                    await interaction.reply(
                        `⏰ You can claim daily bonus in ${remaining} hours!`,
                    );
                    return;
                }

                const dailyBonus = 1000;
                botData.users[userId].balance += dailyBonus;
                botData.users[userId].lastDaily = dailyNow;

                await interaction.reply(
                    `🎁 Daily bonus claimed! +${dailyBonus} :MonthlyPeso:`,
                );
                await saveData();
                break;

            case "balance":
                await interaction.reply(
                    `💰 Your balance: ${botData.users[userId].balance.toLocaleString()} :MonthlyPeso:`,
                );
                break;

            case "debut":
                const debutGroup = options.getString("groupname");
                const debutAlbum = options.getString("albumname");
                const debutCompany = options.getString("company");
                const debutInvestment = options.getInteger("investment");

                if (botData.groups[debutGroup]) {
                    await interaction.reply(
                        `❌ Group "${debutGroup}" already exists!`,
                    );
                    return;
                }

                if (!botData.companies[debutCompany]) {
                    await interaction.reply(
                        `❌ Company "${debutCompany}" not found!`,
                    );
                    return;
                }

                // Check if user owns the company
                if (botData.companies[debutCompany].owner !== userId) {
                    await interaction.reply(
                        `❌ You don't own "${debutCompany}"! Only the company owner can debut groups.`,
                    );
                    return;
                }
                if (debutInvestment <= 0) {
                    await interaction.reply("❌ Investment amount must be positive!");
                    return;
                }

                if (botData.companies[debutCompany].funds < debutInvestment) {
                    await interaction.reply(
                        `❌ ${debutCompany} has insufficient funds!`,
                    );
                    return;
                }

                const company = botData.companies[debutCompany];
                const debutRange = COMPANY_SIZES[company.size].debutPopularity;
                const basePopularity = getRandomInt(debutRange[0], debutRange[1]);
                // Scale investment multiplier more reasonably
                const investmentMultiplier = 1 + (debutInvestment / COMPANY_SIZES[company.size].maxFunds) * 2; // Adjust multiplier scale
                
                const finalPopularity = Math.floor(basePopularity * investmentMultiplier);
                const finalFans = Math.floor(finalPopularity * 0.5);
                const finalFollowers = Math.floor(finalPopularity * 2);

                botData.groups[debutGroup] = {
                    name: debutGroup,
                    company: debutCompany,
                    popularity: finalPopularity,
                    fans: finalFans,
                    followers: finalFollowers,
                    socialMedia: {
                        Titter: Math.floor(finalFollowers * 0.8),
                        TikTak: Math.floor(finalFollowers * 1.2),
                        YouTuboo: Math.floor(finalFollowers * 0.6),
                        Isutagram: Math.floor(finalFollowers),
                    },
                    albums: [{ name: debutAlbum, investment: debutInvestment }], // Albums are just for history now
                    skills: { rap: 50, vocal: 50, dance: 50 },
                    wins: 0,
                    paks: 0,
                    keywords: [debutGroup.toLowerCase()],
                };

                company.funds -= debutInvestment;
                company.groups.push(debutGroup);

                const debutTier = calculateGroupTier(finalPopularity, finalFans);
                updateCharts(); // Update charts after a new group debuts

                await interaction.reply(
                    `🎉 ${debutGroup} debuted under ${debutCompany} with "${debutAlbum}" as a **${debutTier}** tier group! Investment: ${debutInvestment.toLocaleString()} :MonthlyPeso:`,
                );
                await saveData();
                break;

            case "comeback":
                const comebackGroup = options.getString("groupname");
                const comebackAlbum = options.getString("albumname");
                const comebackInvestment = options.getInteger("investment");

                if (!botData.groups[comebackGroup]) {
                    await interaction.reply(
                        `❌ Group "${comebackGroup}" not found!`,
                    );
                    return;
                }

                const group = botData.groups[comebackGroup];
                const groupCompany = botData.companies[group.company];

                // Check if user owns the company
                if (groupCompany.owner !== userId) {
                    await interaction.reply(
                        `❌ You don't own "${group.company}"! Only the company owner can manage comebacks.`,
                    );
                    return;
                }
                if (comebackInvestment <= 0) {
                    await interaction.reply("❌ Investment amount must be positive!");
                    return;
                }

                if (groupCompany.funds < comebackInvestment) {
                    await interaction.reply(
                        `❌ ${groupCompany.name} has insufficient funds!`,
                    );
                    return;
                }

                group.albums.push({
                    name: comebackAlbum,
                    investment: comebackInvestment,
                });
                groupCompany.funds -= comebackInvestment;

                // Boost from comeback is now more impactful
                const boost = Math.floor(comebackInvestment / 500); // More significant boost
                group.popularity += boost;
                group.fans += Math.floor(boost * 0.8);
                group.followers += Math.floor(boost * 1.5);
                
                // Also give a small boost to social media followers
                if (group.socialMedia) {
                    Object.keys(group.socialMedia).forEach(platform => {
                        group.socialMedia[platform] += Math.floor(boost * 0.5);
                    });
                }

                updateCharts(); // Update charts after a comeback

                await interaction.reply(
                    `🎵 ${comebackGroup} made a comeback with "${comebackAlbum}"! Investment: ${comebackInvestment.toLocaleString()} :MonthlyPeso:`,
                );
                await saveData();
                break;

            case "setgroup":
                if (
                    !interaction.member.permissions.has(
                        PermissionFlagsBits.Administrator,
                    )
                ) {
                    await interaction.reply("❌ Admin only command!");
                    return;
                }
                const setGroupName = options.getString("groupname");
                const setCompany = options.getString("company");
                const setDebutAlbum = options.getString("debutalbum");
                const setComebacks = options.getString("comebacks") || "";
                const setTier = options.getString("tier");
                const setPopularity = options.getInteger("popularity");
                const setFans = options.getInteger("fans");
                const setWins = options.getInteger("wins");
                const setPaks = options.getInteger("paks");

                if (botData.groups[setGroupName]) {
                    await interaction.reply(
                        `❌ Group "${setGroupName}" already exists! Use /groupstats to view, or disband and re-add.`,
                    );
                    return;
                }

                if (!botData.companies[setCompany]) {
                    await interaction.reply(
                        `❌ Company "${setCompany}" not found!`,
                    );
                    return;
                }

                const albums = [{ name: setDebutAlbum, investment: 50000 }];
                if (setComebacks) {
                    setComebacks.split(",").forEach((album) => {
                        albums.push({ name: album.trim(), investment: 30000 });
                    });
                }

                let targetTier = setTier;
                let finalPop = setPopularity;
                let finalFans = setFans;
                let finalWins = setWins !== null ? setWins : 0; // Ensure 0 if not set
                let finalPaks = setPaks !== null ? setPaks : 0; // Ensure 0 if not set

                if (!targetTier && (!finalPop || !finalFans)) {
                    // If no tier or explicit popularity/fans, default based on company size
                    const setCompanySize = botData.companies[setCompany].size;
                    targetTier = setCompanySize === 'small' ? 'small' : 
                                setCompanySize === 'medium' ? 'medium' : 'big';
                }

                const albumMultiplier = albums.length;
                const tierStats = {
                    nugu: { pop: 200, fans: 300, skillRange: [40, 60] },
                    small: { pop: 800, fans: 1500, skillRange: [50, 70] },
                    medium: { pop: 4000, fans: 8000, skillRange: [60, 80] },
                    big: { pop: 15000, fans: 35000, skillRange: [70, 90] },
                    icon: { pop: 60000, fans: 150000, skillRange: [80, 95] },
                };

                if (targetTier) {
                    const stats = tierStats[targetTier];
                    finalPop = stats.pop * albumMultiplier;
                    finalFans = stats.fans * albumMultiplier;
                } else {
                    // If popularity/fans explicitly set, use them
                    finalPop = finalPop !== null ? finalPop : 0;
                    finalFans = finalFans !== null ? finalFans : 0;
                    targetTier = calculateGroupTier(finalPop, finalFans); // Calculate tier based on input
                }

                const finalFollowers = Math.floor(finalFans * 2);

                botData.groups[setGroupName] = {
                    name: setGroupName,
                    company: setCompany,
                    popularity: finalPop,
                    fans: finalFans,
                    followers: finalFollowers,
                    socialMedia: {
                        Titter: Math.floor(finalFollowers * 0.8),
                        TikTak: Math.floor(finalFollowers * 1.2),
                        YouTuboo: Math.floor(finalFollowers * 0.6),
                        Isutagram: Math.floor(finalFollowers),
                    },
                    albums: albums,
                    skills: {
                        rap: getRandomInt(tierStats[targetTier].skillRange[0], tierStats[targetTier].skillRange[1]),
                        vocal: getRandomInt(tierStats[targetTier].skillRange[0], tierStats[targetTier].skillRange[1]),
                        dance: getRandomInt(tierStats[targetTier].skillRange[0], tierStats[targetTier].skillRange[1]),
                    },
                    wins: finalWins, // Now takes explicit input, defaults to 0
                    paks: finalPaks, // Now takes explicit input, defaults to 0
                    keywords: [setGroupName.toLowerCase()],
                };

                botData.companies[setCompany].groups.push(setGroupName);
                updateCharts(); // Update charts after setting group

                await interaction.reply(
                    `✅ ${setGroupName} added to the system as a **${targetTier}** tier group with ${albums.length} albums! Pop: ${finalPop.toLocaleString()}, Fans: ${finalFans.toLocaleString()}, Wins: ${finalWins}, PAKs: ${finalPaks}.`,
                );
                await saveData();
                break;

            case "groups":
                const groupsList = Object.keys(botData.groups);
                if (groupsList.length === 0) {
                    await interaction.reply("No groups found!");
                    return;
                }

                const groupsEmbed = new EmbedBuilder()
                    .setTitle("All Groups")
                    .setColor(0xff69b4)
                    .setDescription(groupsList.join("\n"));

                await interaction.reply({ embeds: [groupsEmbed] });
                break;

            case "groupstats":
                const statsGroupName = options.getString("groupname");
                if (!botData.groups[statsGroupName]) {
                    await interaction.reply(
                        `❌ Group "${statsGroupName}" not found!`,
                    );
                    return;
                }

                const statsGroup = botData.groups[statsGroupName];
                const groupTier = calculateGroupTier(
                    statsGroup.popularity,
                    statsGroup.fans,
                );
                const mentions = botData.mentions[statsGroupName] || 0;

                const socialMediaText = statsGroup.socialMedia ? 
                    `🐦 Titter: ${statsGroup.socialMedia.Titter?.toLocaleString() || 0}\n` +
                    `🎵 TikTak: ${statsGroup.socialMedia.TikTak?.toLocaleString() || 0}\n` +
                    `📺 YouTuboo: ${statsGroup.socialMedia.YouTuboo?.toLocaleString() || 0}\n` +
                    `📸 Isutagram: ${statsGroup.socialMedia.Isutagram?.toLocaleString() || 0}` 
                    : "No social media data";

                const statsEmbed = new EmbedBuilder()
                    .setTitle(`📊 ${statsGroupName} Statistics`)
                    .setColor(0xff69b4)
                    .addFields(
                        {
                            name: "🏢 Company",
                            value: statsGroup.company,
                            inline: true,
                        },
                        {
                            name: "⭐ Tier",
                            value:
                                groupTier.charAt(0).toUpperCase() +
                                groupTier.slice(1),
                            inline: true,
                        },
                        {
                            name: "🔥 Popularity",
                            value: statsGroup.popularity.toLocaleString(),
                            inline: true,
                        },
                        {
                            name: "👥 Fans",
                            value: statsGroup.fans.toLocaleString(),
                            inline: true,
                        },
                        {
                            name: "👤 General Followers",
                            value: statsGroup.followers.toLocaleString(),
                            inline: true,
                        },
                        {
                            name: "💿 Albums Released",
                            value: statsGroup.albums.length.toString(),
                            inline: true,
                        },
                        {
                            name: "🎭 Skills",
                            value: `Rap: ${statsGroup.skills.rap} | Vocal: ${statsGroup.skills.vocal} | Dance: ${statsGroup.skills.dance}`,
                        },
                        {
                            name: "🏆 Achievements",
                            value: `Wins: ${statsGroup.wins} | PAKs: ${statsGroup.paks}`,
                        },
                        {
                            name: "📱 Social Media Followers",
                            value: socialMediaText,
                            inline: false,
                        },
                        {
                            name: "📈 Weekly Mentions",
                            value: mentions.toLocaleString(),
                            inline: true,
                        },
                    );

                await interaction.reply({ embeds: [statsEmbed] });
                break;

            case "addwin":
                const winGroup = options.getString("groupname");
                const musicShow = options.getString("musicshow");

                if (!botData.groups[winGroup]) {
                    await interaction.reply(
                        `❌ Group "${winGroup}" not found!`,
                    );
                    return;
                }

                const group_win = botData.groups[winGroup];
                 // Check if user owns the company
                 if (botData.companies[group_win.company].owner !== userId) {
                    await interaction.reply(
                        `❌ You don't own "${group_win.company}"! Only the company owner can add wins.`,
                    );
                    return;
                }

                group_win.wins++;
                group_win.popularity += getRandomInt(500, 2000); // Win boosts popularity
                group_win.fans += getRandomInt(200, 800);

                await interaction.reply(
                    `🏆 ${winGroup} won on ${musicShow}! Total wins: ${group_win.wins}. Popularity and fans increased!`,
                );
                await saveData();
                break;

            case "resetmentions":
                if (
                    !interaction.member.permissions.has(
                        PermissionFlagsBits.Administrator,
                    )
                ) {
                    await interaction.reply("❌ Admin only command!");
                    return;
                }

                const weeksSinceMentionReset =
                    (Date.now() - botData.lastMentionReset) /
                    (1000 * 60 * 60 * 24 * 7);
                if (weeksSinceMentionReset < 1) {
                    await interaction.reply(
                        `❌ Can only reset mentions once per week! Please wait ${Math.ceil(7 - weeksSinceMentionReset * 7)} days.`,
                    );
                    return;
                }

                botData.mentions = {};
                botData.lastMentionReset = Date.now();

                await interaction.reply("✅ All mentions have been reset!");
                await saveData();
                break;

            case "resetcharts":
                if (
                    !interaction.member.permissions.has(
                        PermissionFlagsBits.Administrator,
                    )
                ) {
                    await interaction.reply("❌ Admin only command!");
                    return;
                }

                const weeksSinceChartReset =
                    (Date.now() - botData.lastChartReset) /
                    (1000 * 60 * 60 * 24 * 7);
                if (weeksSinceChartReset < 1) {
                    await interaction.reply(
                        `❌ Can only reset charts once per week! Please wait ${Math.ceil(7 - weeksSinceChartReset * 7)} days.`,
                    );
                    return;
                }

                botData.charts = {
                    MelOn: {},
                    Genies: {},
                    Bugs: {},
                    FLO: {},
                };
                botData.previousCharts = {
                    MelOn: {},
                    Genies: {},
                    Bugs: {},
                    FLO: {},
                };
                botData.lastChartReset = Date.now();
                updateCharts(); // Re-populate charts immediately

                await interaction.reply("✅ All music charts have been reset and re-populated!");
                await saveData();
                break;


            case "brandrep":
                const mentionEntries = Object.entries(botData.mentions);
                mentionEntries.sort((a, b) => b[1] - a[1]);

                const top50 = mentionEntries.slice(0, 50);
                let brandRepText = "";

                if (top50.length === 0) {
                    brandRepText = "No groups have mentions this week.";
                } else {
                    top50.forEach((entry, index) => {
                        brandRepText += `${index + 1}. **${entry[0]}** - ${entry[1].toLocaleString()} mentions\n`;
                    });
                }
                
                const brandRepEmbed = new EmbedBuilder()
                    .setTitle("📈 Brand Reputation Ranking")
                    .setColor(0xff69b4)
                    .setDescription(brandRepText);

                await interaction.reply({ embeds: [brandRepEmbed] });
                break;

            case "addkeywords":
                const keywordGroup = options.getString("groupname");
                const keywords = options.getString("keywords");

                if (!botData.groups[keywordGroup]) {
                    await interaction.reply(
                        `❌ Group "${keywordGroup}" not found!`,
                    );
                    return;
                }
                const group_keywords = botData.groups[keywordGroup];
                 // Check if user owns the company
                 if (botData.companies[group_keywords.company].owner !== userId) {
                    await interaction.reply(
                        `❌ You don't own "${group_keywords.company}"! Only the company owner can add keywords.`,
                    );
                    return;
                }

                const newKeywords = keywords
                    .split(",")
                    .map((k) => k.trim().toLowerCase())
                    .filter(k => k.length > 0 && !group_keywords.keywords.includes(k)); // Avoid adding empty or duplicate keywords
                
                if (newKeywords.length === 0) {
                    await interaction.reply("❌ No new valid keywords to add or keywords already exist.");
                    return;
                }

                botData.groups[keywordGroup].keywords = [
                    ...botData.groups[keywordGroup].keywords,
                    ...newKeywords,
                ];

                await interaction.reply(
                    `✅ Added keywords to ${keywordGroup}: ${newKeywords.join(", ")}`,
                );
                await saveData();
                break;

            case "sales":
                const salesGroup = options.getString("groupname");
                if (!botData.groups[salesGroup]) {
                    await interaction.reply(
                        `❌ Group "${salesGroup}" not found!`,
                    );
                    return;
                }

                const group_sales = botData.groups[salesGroup];
                const company_sales = botData.companies[group_sales.company];

                // Check if user owns the company
                if (company_sales.owner !== userId) {
                    await interaction.reply(
                        `❌ You don't own "${group_sales.company}"! Only the company owner can generate sales.`,
                    );
                    return;
                }

                const tier = calculateGroupTier(
                    group_sales.popularity,
                    group_sales.fans,
                );
                const sales = generateSales(tier, group_sales.popularity);
                const earnings = sales * 2; // Each album sale generates 2 monthly pesos

                company_sales.funds += earnings;
                group_sales.popularity += Math.floor(sales / 500); // Sales slightly boost popularity

                await interaction.reply(
                    `💿 ${salesGroup} sold ${sales.toLocaleString()} albums! ${company_sales.name} earned ${earnings.toLocaleString()} :MonthlyPeso:`,
                );
                await saveData();
                break;

            case "buzz":
                const buzzGroup = options.getString("groupname");
                if (!botData.groups[buzzGroup]) {
                    await interaction.reply(
                        `❌ Group "${buzzGroup}" not found!`,
                    );
                    return;
                }

                const group_buzz = botData.groups[buzzGroup];
                // Check if user owns the company
                if (botData.companies[group_buzz.company].owner !== userId) {
                    await interaction.reply(
                        `❌ You don't own "${group_buzz.company}"! Only the company owner can generate buzz.`,
                    );
                    return;
                }

                const buzzAmount = getRandomInt(100, 1000);
                group_buzz.popularity += buzzAmount;
                group_buzz.fans += Math.floor(buzzAmount * 0.8);
                group_buzz.followers += Math.floor(buzzAmount * 1.2);
                
                // Buzz also gives a small boost to social media followers
                if (group_buzz.socialMedia) {
                    Object.keys(group_buzz.socialMedia).forEach(platform => {
                        group_buzz.socialMedia[platform] += Math.floor(buzzAmount * 0.5);
                    });
                }
                updateCharts(); // Buzz impacts charts

                await interaction.reply(
                    `📈 ${buzzGroup} gained buzz! +${buzzAmount} popularity, +${Math.floor(buzzAmount * 0.8)} fans, +${Math.floor(buzzAmount * 1.2)} followers`,
                );
                await saveData();
                break;

            case "scandal":
                const scandalGroup = options.getString("groupname");
                if (!botData.groups[scandalGroup]) {
                    await interaction.reply(
                        `❌ Group "${scandalGroup}" not found!`,
                    );
                    return;
                }

                const group_scandal = botData.groups[scandalGroup];
                // Check if user owns the company
                if (botData.companies[group_scandal.company].owner !== userId) {
                    await interaction.reply(
                        `❌ You don't own "${group_scandal.company}"! Only the company owner can trigger scandals.`,
                    );
                    return;
                }

                // Give teruakane better luck (80% chance for positive)
                const isPositive = userId === "979346606233104415" ? Math.random() > 0.2 : Math.random() > 0.5;
                const scandalImpact = getRandomInt(500, 2000);
                

                const positiveScandals = [
                    "secretly donated millions to charity! 💝",
                    "member was caught helping elderly people cross the street! 👵",
                    "paid for a fan's medical treatment anonymously! 🏥",
                    "member graduated with honors from university! 🎓",
                    "was spotted volunteering at animal shelter! 🐕",
                    "member learned sign language to communicate with deaf fans! 🤟"
                ];

                const negativeScandals = [
                    "dating rumors with another celebrity surfaced! 💔",
                    "member was caught being rude to staff! 😠",
                    "plagiarism accusations for their latest song! 📝",
                    "member made controversial political statements! 🗳️",
                    "tax evasion allegations emerged! 💸",
                    "member was seen smoking in public! 🚬",
                    "accused of attitude problems during filming! 🎬"
                ];

                if (isPositive) {
                    const scandal = positiveScandals[Math.floor(Math.random() * positiveScandals.length)];
                    group_scandal.popularity += scandalImpact;
                    group_scandal.fans += Math.floor(scandalImpact * 0.6);
                    group_scandal.followers += Math.floor(scandalImpact * 0.8);
                    // Boost social media followers too
                    if (group_scandal.socialMedia) {
                        Object.keys(group_scandal.socialMedia).forEach(platform => {
                            group_scandal.socialMedia[platform] += Math.floor(scandalImpact * 0.4);
                        });
                    }
                    await interaction.reply(
                        `✨ BREAKING: ${scandalGroup} ${scandal}\n📈 +${scandalImpact} popularity boost!`,
                    );
                } else {
                    const scandal = negativeScandals[Math.floor(Math.random() * negativeScandals.length)];
                    group_scandal.popularity = Math.max(
                        0,
                        group_scandal.popularity - scandalImpact,
                    );
                    group_scandal.fans = Math.max(
                        0,
                        group_scandal.fans - Math.floor(scandalImpact * 0.6),
                    );
                    group_scandal.followers = Math.max(
                        0,
                        group_scandal.followers -
                            Math.floor(scandalImpact * 0.8),
                    );
                    // Decrease social media followers too
                    if (group_scandal.socialMedia) {
                        Object.keys(group_scandal.socialMedia).forEach(platform => {
                            group_scandal.socialMedia[platform] = Math.max(0, 
                                group_scandal.socialMedia[platform] - Math.floor(scandalImpact * 0.4));
                        });
                    }
                    await interaction.reply(
                        `💥 SCANDAL: ${scandalGroup} ${scandal}\n📉 -${scandalImpact} popularity loss!`,
                    );
                }

                updateCharts(); // Scandal can impact chart positions
                await saveData();
                break;

            case "performance":
                const perfGroup = options.getString("groupname");
                if (!botData.groups[perfGroup]) {
                    await interaction.reply(
                        `❌ Group "${perfGroup}" not found!`,
                    );
                    return;
                }

                const group_perf = botData.groups[perfGroup];
                
                // Check if user owns the company
                if (botData.companies[group_perf.company].owner !== userId) {
                    await interaction.reply(
                        `❌ You don't own "${group_perf.company}"! Only the company owner can use performance.`,
                    );
                    return;
                }
                const avgSkill =
                    (group_perf.skills.rap +
                        group_perf.skills.vocal +
                        group_perf.skills.dance) /
                    3;

                let performanceLevel;
                let reward;

                // Give teruakane better performance results
                const skillBonus = userId === "979346606233104415" ? 15 : 0;
                const adjustedSkill = avgSkill + skillBonus;

                if (adjustedSkill >= 80) {
                    performanceLevel = "Perfect";
                    reward = { popularity: 800, fans: 600, money: 5000 };
                } else if (adjustedSkill >= 60) {
                    performanceLevel = "Good";
                    reward = { popularity: 400, fans: 300, money: 2000 };
                } else {
                    performanceLevel = "Bad";
                    reward = { popularity: 100, fans: 50, money: 500 };
                }

                // Extra bonus for teruakane
                if (userId === "979346606233104415") {
                    reward.popularity = Math.floor(reward.popularity * 1.3);
                    reward.fans = Math.floor(reward.fans * 1.3);
                    reward.money = Math.floor(reward.money * 1.3);
                }

                group_perf.popularity += reward.popularity;
                group_perf.fans += reward.fans;
                // Performance also boosts social media followers slightly
                if (group_perf.socialMedia) {
                    Object.keys(group_perf.socialMedia).forEach(platform => {
                        group_perf.socialMedia[platform] += Math.floor(reward.popularity * 0.3);
                    });
                }
                botData.companies[group_perf.company].funds += reward.money;
                updateCharts(); // Performance impacts charts

                await interaction.reply(
                    `🎤 ${perfGroup} had a ${performanceLevel} performance! +${reward.popularity} popularity, +${reward.fans} fans, +${reward.money.toLocaleString()} :MonthlyPeso: to ${group_perf.company}`,
                );
                await saveData();
                break;

            case "trainvocals":
                const vocalGroup = options.getString("groupname");
                if (!botData.groups[vocalGroup]) {
                    await interaction.reply(
                        `❌ Group "${vocalGroup}" not found!`,
                    );
                    return;
                }

                const group_vocal = botData.groups[vocalGroup];
                const company_vocal = botData.companies[group_vocal.company];

                // Check if user owns the company
                if (company_vocal.owner !== userId) {
                    await interaction.reply(
                        `❌ You don't own "${group_vocal.company}"! Only the company owner can train groups.`,
                    );
                    return;
                }

                const trainingCost = 5000;

                if (company_vocal.funds < trainingCost) {
                    await interaction.reply(
                        `❌ ${group_vocal.company} has insufficient funds for training!`,
                    );
                    return;
                }

                if (group_vocal.skills.vocal >= 100) {
                    await interaction.reply(
                        `❌ ${vocalGroup}'s vocal skill is already maxed out!`,
                    );
                    return;
                }

                company_vocal.funds -= trainingCost;
                const skillIncrease = userId === "979346606233104415" ? getRandomInt(3, 7) : getRandomInt(1, 5);
                group_vocal.skills.vocal = Math.min(100, group_vocal.skills.vocal + skillIncrease);
                
                // Training also gives small popularity boost
                const trainingBoost = skillIncrease * 10;
                group_vocal.popularity += trainingBoost;
                group_vocal.fans += Math.floor(trainingBoost * 0.5);

                const oldTier = calculateGroupTier(group_vocal.popularity - trainingBoost, group_vocal.fans - Math.floor(trainingBoost * 0.5));
                const newTier = calculateGroupTier(group_vocal.popularity, group_vocal.fans);

                let replyMessage = `🎵 ${vocalGroup} trained vocals! New level: ${group_vocal.skills.vocal}/100 (+${skillIncrease}) | +${trainingBoost} popularity`;
                
                if (oldTier !== newTier) {
                    replyMessage += `\n🔥 **TIER UP!** ${vocalGroup} advanced from ${oldTier.toUpperCase()} to ${newTier.toUpperCase()}!`;
                }
                updateCharts(); // Training also impacts charts

                await interaction.reply(replyMessage);
                await saveData();
                break;

            case "traindance":
                const danceGroup = options.getString("groupname");
                if (!botData.groups[danceGroup]) {
                    await interaction.reply(
                        `❌ Group "${danceGroup}" not found!`,
                    );
                    return;
                }

                const group_dance = botData.groups[danceGroup];
                const company_dance = botData.companies[group_dance.company];

                // Check if user owns the company
                if (company_dance.owner !== userId) {
                    await interaction.reply(
                        `❌ You don't own "${group_dance.company}"! Only the company owner can train groups.`,
                    );
                    return;
                }

                const danceTrainingCost = 5000;

                if (company_dance.funds < danceTrainingCost) {
                    await interaction.reply(
                        `❌ ${group_dance.company} has insufficient funds for training!`,
                    );
                    return;
                }

                if (group_dance.skills.dance >= 100) {
                    await interaction.reply(
                        `❌ ${danceGroup}'s dance skill is already maxed out!`,
                    );
                    return;
                }

                company_dance.funds -= danceTrainingCost;
                const danceSkillIncrease = userId === "979346606233104415" ? getRandomInt(3, 7) : getRandomInt(1, 5);
                group_dance.skills.dance = Math.min(100, group_dance.skills.dance + danceSkillIncrease);
                
                // Training also gives small popularity boost
                const danceTrainingBoost = danceSkillIncrease * 10;
                group_dance.popularity += danceTrainingBoost;
                group_dance.fans += Math.floor(danceTrainingBoost * 0.5);

                const danceOldTier = calculateGroupTier(group_dance.popularity - danceTrainingBoost, group_dance.fans - Math.floor(danceTrainingBoost * 0.5));
                const danceNewTier = calculateGroupTier(group_dance.popularity, group_dance.fans);

                let danceReplyMessage = `💃 ${danceGroup} trained dance! New level: ${group_dance.skills.dance}/100 (+${danceSkillIncrease}) | +${danceTrainingBoost} popularity`;
                
                if (danceOldTier !== danceNewTier) {
                    danceReplyMessage += `\n🔥 **TIER UP!** ${danceGroup} advanced from ${danceOldTier.toUpperCase()} to ${danceNewTier.toUpperCase()}!`;
                }
                updateCharts(); // Training also impacts charts

                await interaction.reply(danceReplyMessage);
                await saveData();
                break;

            case "trainrap":
                const rapGroup = options.getString("groupname");
                if (!botData.groups[rapGroup]) {
                    await interaction.reply(
                        `❌ Group "${rapGroup}" not found!`,
                    );
                    return;
                }

                const group_rap = botData.groups[rapGroup];
                const company_rap = botData.companies[group_rap.company];

                // Check if user owns the company
                if (company_rap.owner !== userId) {
                    await interaction.reply(
                        `❌ You don't own "${group_rap.company}"! Only the company owner can train groups.`,
                    );
                    return;
                }

                const rapTrainingCost = 5000;

                if (company_rap.funds < rapTrainingCost) {
                    await interaction.reply(
                        `❌ ${group_rap.company} has insufficient funds for training!`,
                    );
                    return;
                }

                if (group_rap.skills.rap >= 100) {
                    await interaction.reply(
                        `❌ ${rapGroup}'s rap skill is already maxed out!`,
                    );
                    return;
                }

                company_rap.funds -= rapTrainingCost;
                const rapSkillIncrease = userId === "979346606233104415" ? getRandomInt(3, 7) : getRandomInt(1, 5);
                group_rap.skills.rap = Math.min(100, group_rap.skills.rap + rapSkillIncrease);
                
                // Training also gives small popularity boost
                const rapTrainingBoost = rapSkillIncrease * 10;
                group_rap.popularity += rapTrainingBoost;
                group_rap.fans += Math.floor(rapTrainingBoost * 0.5);

                const rapOldTier = calculateGroupTier(group_rap.popularity - rapTrainingBoost, group_rap.fans - Math.floor(rapTrainingBoost * 0.5));
                const rapNewTier = calculateGroupTier(group_rap.popularity, group_rap.fans);

                let rapReplyMessage = `🎤 ${rapGroup} trained rap! New level: ${group_rap.skills.rap}/100 (+${rapSkillIncrease}) | +${rapTrainingBoost} popularity`;
                
                if (rapOldTier !== rapNewTier) {
                    rapReplyMessage += `\n🔥 **TIER UP!** ${rapGroup} advanced from ${rapOldTier.toUpperCase()} to ${rapNewTier.toUpperCase()}!`;
                }
                updateCharts(); // Training also impacts charts

                await interaction.reply(rapReplyMessage);
                await saveData();
                break;

            case "sponsorship":
                const sponsorGroup = options.getString("groupname");
                if (!botData.groups[sponsorGroup]) {
                    await interaction.reply(
                        `❌ Group "${sponsorGroup}" not found!`,
                    );
                    return;
                }

                const group_sponsor = botData.groups[sponsorGroup];
                const company_sponsor = botData.companies[group_sponsor.company];

                // Check if user owns the company
                if (company_sponsor.owner !== userId) {
                    await interaction.reply(
                        `❌ You don't own "${group_sponsor.company}"! Only the company owner can attempt sponsorships.`,
                    );
                    return;
                }

                const tier_sponsor = calculateGroupTier(
                    group_sponsor.popularity,
                    group_sponsor.fans,
                );
                // Adjust success rates to be more reasonable
                let successRate = { nugu: 0.05, small: 0.2, medium: 0.5, big: 0.75, icon: 0.9 }[
                    tier_sponsor
                ];

                // Give teruakane better sponsorship luck
                if (userId === "979346606233104415") {
                    successRate = Math.min(0.99, successRate + 0.15); // Slightly better bonus
                }

                if (Math.random() < successRate) {
                    // Reward scales better with tier
                    const sponsorReward = {
                        nugu: getRandomInt(500, 2000),
                        small: getRandomInt(5000, 20000),
                        medium: getRandomInt(20000, 75000),
                        big: getRandomInt(75000, 250000),
                        icon: getRandomInt(250000, 1000000),
                    }[tier_sponsor];

                    company_sponsor.funds += sponsorReward;
                    group_sponsor.popularity += Math.floor(sponsorReward / 500); // Sponsor boosts popularity more significantly
                    group_sponsor.followers += Math.floor(sponsorReward / 100); // Boost general followers
                    // Boost social media followers
                    if (group_sponsor.socialMedia) {
                        Object.keys(group_sponsor.socialMedia).forEach(platform => {
                            group_sponsor.socialMedia[platform] += Math.floor(sponsorReward / 200);
                        });
                    }

                    await interaction.reply(
                        `🤝 ${sponsorGroup} landed a sponsorship! +${sponsorReward.toLocaleString()} :MonthlyPeso: to ${company_sponsor.name} and significant popularity and follower gains!`,
                    );
                } else {
                    await interaction.reply(
                        `❌ ${sponsorGroup} failed to land a sponsorship this time. (Success Chance: ${Math.round(successRate * 100)}%)`,
                    );
                }

                await saveData();
                break;

            case "stream":
                const streamGroup = options.getString("groupname");
                const streamAlbum = options.getString("albumname"); // Still needed for the album name in message

                // Check cooldown
                const lastStream = botData.users[userId].lastStream || 0;
                const streamNow = Date.now();
                if (streamNow - lastStream < 180000) { // 3 minutes cooldown
                    const remaining = Math.ceil((180000 - (streamNow - lastStream)) / 1000);
                    await interaction.reply(
                        `⏰ You can stream again in ${remaining} seconds!`,
                    );
                    return;
                }

                if (!botData.groups[streamGroup]) {
                    await interaction.reply(
                        `❌ Group "${streamGroup}" not found!`,
                    );
                    return;
                }

                const group_stream = botData.groups[streamGroup];
                const albumExists = group_stream.albums.some( // Still check for album existence
                    (album) =>
                        album.name.toLowerCase() === streamAlbum.toLowerCase(),
                );

                if (!albumExists) {
                    await interaction.reply(
                        `❌ Album "${streamAlbum}" not found for ${streamGroup}!`,
                    );
                    return;
                }

                const groupTier_stream = calculateGroupTier(group_stream.popularity, group_stream.fans);
                const streamBoosts = {
                    nugu: getRandomInt(5, 25), // Reduced for Nugu
                    small: getRandomInt(10, 50),
                    medium: getRandomInt(20, 100),
                    big: getRandomInt(50, 200),
                    icon: getRandomInt(100, 400),
                };

                const streamBoost = streamBoosts[groupTier_stream];
                group_stream.popularity += streamBoost;
                group_stream.fans += Math.floor(streamBoost * 0.3);
                
                botData.users[userId].lastStream = streamNow;

                updateCharts(); // Streams now directly influence charts through popularity

                await interaction.reply(
                    `📱 You streamed "${streamAlbum}" by ${streamGroup}! +${streamBoost} popularity and +${Math.floor(streamBoost * 0.3)} fans! This will help them on the charts!`,
                );
                await saveData();
                break;

            case "charts":
                const chartGroup = options.getString("groupname");

                if (chartGroup) {
                    if (!botData.groups[chartGroup]) {
                        await interaction.reply(
                            `❌ Group "${chartGroup}" not found!`,
                        );
                        return;
                    }

                    let groupCharts = "";
                    let isCharting = false;
                    CHART_PLATFORMS.forEach((platform) => {
                        if (botData.charts[platform][chartGroup]) {
                            const position = botData.charts[platform][chartGroup];
                            const indicator = getPositionIndicator(chartGroup, platform);
                            groupCharts += `${platform}: #${position}${indicator}\n`;
                            isCharting = true;
                        }
                    });

                    if (!isCharting) {
                        await interaction.reply(
                            `${chartGroup} is not currently charting.`,
                        );
                        return;
                    }

                    const chartEmbed = new EmbedBuilder()
                        .setTitle(`${chartGroup} Chart Positions`)
                        .setColor(0xff69b4)
                        .setDescription(groupCharts);

                    await interaction.reply({ embeds: [chartEmbed] });
                } else {
                    let allCharts = "";
                    CHART_PLATFORMS.forEach((platform) => {
                        allCharts += `**${platform}**\n`;
                        const sortedEntries = Object.entries(
                            botData.charts[platform],
                        ).sort((a, b) => a[1] - b[1]); // Sort by position ascending
                        
                        if (sortedEntries.length === 0) {
                            allCharts += "No groups charting.\n";
                        } else {
                            sortedEntries.slice(0, 100).forEach(([groupName, position]) => {
                                const indicator = getPositionIndicator(groupName, platform);
                                allCharts += `#${position} - ${groupName}${indicator}\n`;
                            });
                        }
                        allCharts += "\n";
                    });

                    const allChartsEmbed = new EmbedBuilder()
                        .setTitle("📊 Music Charts (TOP 100)")
                        .setColor(0xff69b4)
                        .setDescription(
                            allCharts || "No songs currently charting",
                        );

                    await interaction.reply({ embeds: [allChartsEmbed] });
                }
                break;

            case "payola":
                const payolaGroup = options.getString("groupname");
                const payolaAmount = options.getInteger("amount");

                if (!botData.groups[payolaGroup]) {
                    await interaction.reply(
                        `❌ Group "${payolaGroup}" not found!`,
                    );
                    return;
                }

                if (payolaAmount < 1000000) {
                    await interaction.reply(
                        "❌ Minimum payola amount is 1,000,000 :MonthlyPeso:!",
                    );
                    return;
                }

                const group_payola = botData.groups[payolaGroup];
                const company_payola = botData.companies[group_payola.company];

                // Check if user owns the company
                if (company_payola.owner !== userId) {
                    await interaction.reply(
                        `❌ You don't own "${group_payola.company}"! Only the company owner can use payola.`,
                    );
                    return;
                }

                if (company_payola.funds < payolaAmount) {
                    await interaction.reply(
                        `❌ ${company_payola.name} has insufficient funds!`,
                    );
                    return;
                }

                company_payola.funds -= payolaAmount;

                // Improve chart positions based on payola amount
                let chartImpacted = false;
                CHART_PLATFORMS.forEach((platform) => {
                    if (botData.charts[platform][payolaGroup]) {
                        const currentPosition = botData.charts[platform][payolaGroup];
                        const improvement = Math.floor(payolaAmount / 50000); // 1 position for every 50,000
                        const newPosition = Math.max(1, currentPosition - improvement);
                        botData.charts[platform][payolaGroup] = newPosition;
                        chartImpacted = true;
                    }
                });

                // If group wasn't charting, give them a chance to enter high
                if (!chartImpacted) {
                    CHART_PLATFORMS.forEach(platform => {
                        if (Math.random() < 0.7) { // 70% chance to enter chart if not already
                            let position = getRandomInt(1, Math.floor(100 - (payolaAmount / 20000))); // Higher amount means better entry
                            position = Math.max(1, Math.min(100, position)); // Ensure it's within 1-100
                             // Ensure no duplicate positions
                            const currentChartValues = Object.values(botData.charts[platform]);
                            while (currentChartValues.includes(position) && position <= 100) {
                                position = getRandomInt(1, 100);
                            }
                            if (position <= 100) {
                                botData.charts[platform][payolaGroup] = position;
                                chartImpacted = true;
                            }
                        }
                    });
                }

                // Boost popularity significantly
                const popularityBoost = Math.floor(payolaAmount / 500);
                group_payola.popularity += popularityBoost;
                group_payola.fans += Math.floor(popularityBoost * 0.5);
                group_payola.followers += Math.floor(popularityBoost * 0.8);
                // Also social media followers
                if (group_payola.socialMedia) {
                    Object.keys(group_payola.socialMedia).forEach(platform => {
                        group_payola.socialMedia[platform] += Math.floor(popularityBoost * 0.4);
                    });
                }


                // Check for PAK after payola (and update charts to ensure positions are correct)
                updateCharts(); // Re-sort and finalize chart positions before checking PAK
                if (checkPAK(payolaGroup)) {
                    group_payola.paks++;
                    await interaction.reply(
                        `💰 ${payolaGroup} used payola (${payolaAmount.toLocaleString()} :MonthlyPeso:) and achieved a PAK! 🏆✨ This is a rare and special achievement!`,
                    );
                } else {
                    await interaction.reply(
                        `💰 ${payolaGroup} used payola (${payolaAmount.toLocaleString()} :MonthlyPeso:) to improve chart positions and gained ${popularityBoost.toLocaleString()} popularity!`,
                    );
                }

                await saveData();
                break;

            case "newpost":
                const postGroup = options.getString("groupname");
                const platform = options.getString("platform");

                if (!botData.groups[postGroup]) {
                    await interaction.reply(`❌ Group "${postGroup}" not found!`);
                    return;
                }

                const group_post = botData.groups[postGroup];
                
                // Check if user owns the company
                if (botData.companies[group_post.company].owner !== userId) {
                    await interaction.reply(
                        `❌ You don't own "${group_post.company}"! Only the company owner can make posts.`,
                    );
                    return;
                }
                const groupTier_post = calculateGroupTier(group_post.popularity, group_post.fans);

                // Initialize social media if doesn't exist (should be handled in loadData, but for safety)
                if (!group_post.socialMedia) {
                    group_post.socialMedia = {
                        Titter: Math.floor(group_post.followers * 0.8),
                        TikTak: Math.floor(group_post.followers * 1.2),
                        YouTuboo: Math.floor(group_post.followers * 0.6),
                        Isutagram: Math.floor(group_post.followers),
                    };
                }

                // Base engagement values, scaled appropriately
                const baseEngagement = {
                    nugu: { likes: [5, 50], comments: [1, 10] },
                    small: { likes: [50, 500], comments: [5, 50] },
                    medium: { likes: [500, 5000], comments: [50, 500] },
                    big: { likes: [5000, 50000], comments: [500, 5000] },
                    icon: { likes: [20000, 200000], comments: [2000, 20000] },
                }[groupTier_post];

                // Platform multipliers (already defined, just for clarity)
                const platformMultiplier = {
                    Titter: 0.8,
                    TikTak: 1.5,
                    YouTuboo: 0.6,
                    Isutagram: 1.2,
                }[platform];

                const likes = Math.floor(getRandomInt(baseEngagement.likes[0], baseEngagement.likes[1]) * platformMultiplier);
                const comments = Math.floor(getRandomInt(baseEngagement.comments[0], baseEngagement.comments[1]) * platformMultiplier);

                // Calculate gains based on engagement (adjust multipliers for impact)
                const fanGain = Math.floor(likes * 0.005 + comments * 0.02); // Reduced impact
                const followerGain = Math.floor(likes * 0.003 + comments * 0.015); // Reduced impact
                const socialMediaGain = Math.floor(likes * 0.008 + comments * 0.025); // Reduced impact

                // Apply gains
                group_post.fans += fanGain;
                group_post.followers += followerGain; // General followers too
                group_post.socialMedia[platform] += socialMediaGain;
                group_post.popularity += Math.floor(fanGain * 0.5 + followerGain * 0.2); // Popularity from fans and followers
                
                // Add to mentions for brand reputation
                botData.mentions[postGroup] = (botData.mentions[postGroup] || 0) + Math.floor(comments * 0.5 + likes * 0.05); // More mentions from comments/likes
                updateCharts(); // Social media activity can impact charts

                const platformEmojis = {
                    Titter: "🐦",
                    TikTak: "🎵",
                    YouTuboo: "📺",
                    Isutagram: "📸"
                };

                await interaction.reply(
                    `${platformEmojis[platform]} ${postGroup} just made a new ${platform} post!\n` +
                    `❤️ Likes: ${likes.toLocaleString()}\n` +
                    `💬 Comments: ${comments.toLocaleString()}\n` +
                    `👥 Fans: ${group_post.fans.toLocaleString()} (+${fanGain})\n` +
                    `👤 General Followers: ${group_post.followers.toLocaleString()} (+${followerGain})\n` +
                    `${platformEmojis[platform]} ${platform} Followers: ${group_post.socialMedia[platform].toLocaleString()} (+${socialMediaGain})`
                );

                await saveData();
                break;

            case "companies":
                const companiesList = Object.entries(botData.companies);
                if (companiesList.length === 0) {
                    await interaction.reply("No companies found!");
                    return;
                }

                let companiesText = "";
                for (const [companyName, company] of companiesList) {
                    const groupsWithTiers = company.groups.map(groupName => {
                        const group = botData.groups[groupName];
                        if (group) {
                            const tier = calculateGroupTier(group.popularity, group.fans);
                            return `${groupName} (${tier})`;
                        }
                        return groupName;
                    });

                    companiesText += `**${companyName}** (${company.size} company)\n`;
                    companiesText += `📊 Funds: ${company.funds.toLocaleString()} :MonthlyPeso:\n`;
                    let ownerMention = 'None';
                    if (company.owner) {
                        try {
                            const ownerUser = await client.users.fetch(company.owner);
                            ownerMention = `<@${ownerUser.id}> (${ownerUser.username})`;
                        } catch (e) {
                            ownerMention = `<@${company.owner}> (User not found)`;
                        }
                    }
                    companiesText += `👑 Owner: ${ownerMention}\n`;
                    companiesText += `🎵 Groups: ${groupsWithTiers.join(", ") || "None"}\n\n`;
                }

                const companiesEmbed = new EmbedBuilder()
                    .setTitle("🏢 All Companies")
                    .setColor(0xff69b4)
                    .setDescription(companiesText);

                await interaction.reply({ embeds: [companiesEmbed] });
                break;

            case "buyfollowers":
                const buyGroup = options.getString("groupname");
                const buyPlatform = options.getString("platform");
                const buyAmount = options.getInteger("amount");

                if (!botData.groups[buyGroup]) {
                    await interaction.reply(`❌ Group "${buyGroup}" not found!`);
                    return;
                }

                if (buyAmount < 100) { // Lower minimum
                    await interaction.reply(
                        "❌ Minimum purchase is 100 followers!",
                    );
                    return;
                }

                const group_buy = botData.groups[buyGroup];
                const company_buy = botData.companies[group_buy.company];

                // Check if user owns the company
                if (company_buy.owner !== userId) {
                    await interaction.reply(
                        `❌ You don't own "${group_buy.company}"! Only the company owner can buy followers.`,
                    );
                    return;
                }

                // Cheaper: 10 pesos per follower (was 500)
                const followerCost = buyAmount * 10;

                if (company_buy.funds < followerCost) {
                    await interaction.reply(
                        `❌ ${company_buy.name} has insufficient funds! Cost: ${followerCost.toLocaleString()} :MonthlyPeso:`,
                    );
                    return;
                }

                // Initialize social media if doesn't exist (should be handled in loadData)
                if (!group_buy.socialMedia) {
                    group_buy.socialMedia = {
                        Titter: Math.floor(group_buy.followers * 0.8),
                        TikTak: Math.floor(group_buy.followers * 1.2),
                        YouTuboo: Math.floor(group_buy.followers * 0.6),
                        Isutagram: Math.floor(group_buy.followers),
                    };
                }

                company_buy.funds -= followerCost;
                group_buy.socialMedia[buyPlatform] += buyAmount;

                // Small boost to general followers and popularity
                const generalBoost = Math.floor(buyAmount * 0.1);
                group_buy.followers += generalBoost;
                group_buy.popularity += Math.floor(generalBoost * 0.5);
                updateCharts(); // Buying followers can impact charts

                const platformEmojis = {
                    Titter: "🐦",
                    TikTak: "🎵",
                    YouTuboo: "📺",
                    Isutagram: "📸"
                };

                await interaction.reply(
                    `${platformEmojis[buyPlatform]} ${buyGroup} bought ${buyAmount.toLocaleString()} ${buyPlatform} followers for ${followerCost.toLocaleString()} :MonthlyPeso:!\n` +
                    `${platformEmojis[buyPlatform]} ${buyPlatform}: ${group_buy.socialMedia[buyPlatform].toLocaleString()} followers\n` +
                    `👤 General Followers: ${group_buy.followers.toLocaleString()} (+${generalBoost})`
                );

                await saveData();
                break;
            
            case "disbandgroup":
                const disbandGroupName = options.getString("groupname");
                const disbandGroup = botData.groups[disbandGroupName];

                if (!disbandGroup) {
                    await interaction.reply(`❌ Group "${disbandGroupName}" not found!`);
                    return;
                }

                const disbandCompany = botData.companies[disbandGroup.company];

                // Check if user owns the company
                if (disbandCompany.owner !== userId) {
                    await interaction.reply(
                        `❌ You don't own "${disbandCompany.name}"! Only the company owner can disband groups.`,
                    );
                    return;
                }

                // Remove group from company's group list
                disbandCompany.groups = disbandCompany.groups.filter(
                    (groupName) => groupName !== disbandGroupName
                );

                // Remove group from botData.groups
                delete botData.groups[disbandGroupName];

                // Remove group from charts
                CHART_PLATFORMS.forEach(platform => {
                    if (botData.charts[platform][disbandGroupName]) {
                        delete botData.charts[platform][disbandGroupName];
                    }
                    if (botData.previousCharts[platform][disbandGroupName]) {
                        delete botData.previousCharts[platform][disbandGroupName];
                    }
                });

                // Remove group from mentions
                if (botData.mentions[disbandGroupName]) {
                    delete botData.mentions[disbandGroupName];
                }
                
                await interaction.reply(`🗑️ Group "${disbandGroupName}" has been disbanded!`);
                await saveData();
                break;

            case "closecompany":
                const closeCompanyName = options.getString("companyname");
                const closeCompany = botData.companies[closeCompanyName];

                if (!closeCompany) {
                    await interaction.reply(`❌ Company "${closeCompanyName}" not found!`);
                    return;
                }

                // Check if user owns the company
                if (closeCompany.owner !== userId) {
                    await interaction.reply(
                        `❌ You don't own "${closeCompanyName}"! Only the company owner can close the company.`,
                    );
                    return;
                }

                // Disband all groups under this company
                if (closeCompany.groups && closeCompany.groups.length > 0) {
                    for (const groupName of [...closeCompany.groups]) { // Iterate over a copy
                        const group = botData.groups[groupName];
                        if (group) {
                            delete botData.groups[groupName];
                            // Remove group from charts
                            CHART_PLATFORMS.forEach(platform => {
                                if (botData.charts[platform][groupName]) {
                                    delete botData.charts[platform][groupName];
                                }
                                if (botData.previousCharts[platform][groupName]) {
                                    delete botData.previousCharts[platform][groupName];
                                }
                            });
                            // Remove group from mentions
                            if (botData.mentions[groupName]) {
                                delete botData.mentions[groupName];
                            }
                        }
                    }
                }

                // Give back a percentage of funds to the owner
                const refundPercentage = 0.5; // 50% refund
                const refundAmount = Math.floor(closeCompany.funds * refundPercentage);
                botData.users[userId].balance += refundAmount;

                // Delete the company
                delete botData.companies[closeCompanyName];

                await interaction.reply(
                    `🗑️ Company "${closeCompanyName}" has been closed! All associated groups disbanded. You received ${refundAmount.toLocaleString()} :MonthlyPeso: from the company's remaining funds.`,
                );
                await saveData();
                break;


            default:
                await interaction.reply("❌ Unknown command!");
        }
    } catch (error) {
        console.error("Command error:", error);
        await interaction.reply(
            "❌ An error occurred while processing the command. Please try again later.",
        );
    }
});

// Message listener for mentions
client.on("messageCreate", (message) => {
    if (message.author.bot) return;

    const content = message.content.toLowerCase();

    Object.values(botData.groups).forEach((group) => {
        if (group.keywords && Array.isArray(group.keywords)) {
            group.keywords.forEach((keyword) => {
                // Ensure keyword is a string and not empty
                if (typeof keyword === 'string' && keyword.length > 0 && content.includes(keyword)) {
                    botData.mentions[group.name] =
                        (botData.mentions[group.name] || 0) + 1;
                }
            });
        }
    });

    saveData();
});

// Bot startup
client.once("ready", async () => {
    console.log(`${client.user.tag} is online!`);

    await loadData();

    // Register slash commands
    try {
        console.log("Registering slash commands...");
        await client.application.commands.set(commands);
        console.log("Slash commands registered successfully!");
    } catch (error) {
        console.error("Error registering commands:", error);
    }

    // Auto-save every 5 minutes
    setInterval(saveData, 300000);

    // Update charts every hour
    setInterval(updateCharts, 3600000);
});

// Error handling
client.on("error", (error) => {
    console.error("Discord client error:", error);
});

process.on("unhandledRejection", (error) => {
    console.error("Unhandled promise rejection:", error);
});

// Login with bot token
client.login(process.env.BOT_TOKEN);
